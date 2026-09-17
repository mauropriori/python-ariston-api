"""Tests for cloud request throttling and failure handling."""

import asyncio
import threading
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from ariston_net_api.ariston_api import (
    AristonAPI,
    ConnectionException,
    RateLimitException,
    _parse_retry_after_seconds,
)
from ariston_net_api.const import ARISTON_LOGIN, DeviceAttribute
from ariston_net_api.galevo_device import AristonGalevoDevice


class _AsyncResponse:
    def __init__(self, status, content=b"", json_data=None, headers=None):
        self.status = status
        self.ok = status < 400
        self.headers = headers or {}
        self._content = content
        self._json_data = json_data

    async def read(self):
        return self._content

    async def json(self):
        return self._json_data


def _session_factory(responses, sessions):
    class _Session:
        def __init__(self, *, timeout):
            self.timeout = timeout
            sessions.append(self)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def request(self, *args, **kwargs):
            return responses.pop(0)

    return _Session


class CloudResilienceTests(TestCase):
    def setUp(self):
        AristonAPI._request_states.clear()
        AristonAPI._request_locks.clear()
        AristonAPI._auth_locks.clear()

    def test_retry_after_parser_prefers_header_then_body(self):
        self.assertEqual(
            _parse_retry_after_seconds(
                {"Retry-After": "12"}, b"Requests are blocked for 66 seconds"
            ),
            12,
        )
        self.assertEqual(
            _parse_retry_after_seconds({}, b"Requests are blocked for 66 seconds"),
            66,
        )
        self.assertEqual(_parse_retry_after_seconds({}, b"Too many requests"), 60)

    @patch("ariston_net_api.ariston_api.time.sleep")
    @patch("ariston_net_api.ariston_api.requests.request")
    def test_sync_500_is_not_retried(self, request, _sleep):
        response = MagicMock(
            ok=False,
            status_code=500,
            content=b"server error",
            headers={},
        )
        request.return_value = response

        with self.assertRaises(ConnectionException):
            AristonAPI("user", "pass")._get("https://example.test/state")

        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args.kwargs["timeout"], 30)

    @patch("ariston_net_api.ariston_api.time.sleep")
    @patch("ariston_net_api.ariston_api.requests.request")
    def test_sync_429_waits_and_retries_once(self, request, sleep):
        limited = MagicMock(
            ok=False,
            status_code=429,
            content=b"Requests are blocked for 4 seconds",
            headers={},
        )
        success = MagicMock(
            ok=True,
            status_code=200,
            content=b"{}",
            headers={},
        )
        success.json.return_value = {"ok": True}
        request.side_effect = [limited, success]

        result = AristonAPI("user", "pass")._get("https://example.test/state")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(request.call_count, 2)
        self.assertGreaterEqual(sleep.call_count, 1)
        self.assertEqual(sleep.call_args_list[0].args[0], 6)

    def test_async_500_is_not_retried_and_has_30_second_timeout(self):
        sessions = []
        responses = [_AsyncResponse(500, b"server error")]

        async def run():
            with patch(
                "ariston_net_api.ariston_api.aiohttp.ClientSession",
                _session_factory(responses, sessions),
            ), self.assertRaises(ConnectionException):
                await AristonAPI("user", "pass")._async_get(
                    "https://example.test/state"
                )

        asyncio.run(run())

        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0].timeout.total, 30)

    def test_async_429_waits_and_retries_once(self):
        sessions = []
        responses = [
            _AsyncResponse(429, b"Requests are blocked for 4 seconds"),
            _AsyncResponse(200, b"{}", {"ok": True}),
        ]

        async def run():
            with patch(
                "ariston_net_api.ariston_api.aiohttp.ClientSession",
                _session_factory(responses, sessions),
            ), patch(
                "ariston_net_api.ariston_api.asyncio.sleep", new_callable=AsyncMock
            ) as sleep:
                result = await AristonAPI("user", "pass")._async_get(
                    "https://example.test/state"
                )
                self.assertEqual(sleep.await_args_list[0].args[0], 6)
                return result

        self.assertEqual(asyncio.run(run()), {"ok": True})
        self.assertEqual(len(sessions), 2)

    def test_async_429_is_retried_only_once(self):
        sessions = []
        responses = [
            _AsyncResponse(429, b"Requests are blocked for 4 seconds"),
            _AsyncResponse(429, b"Requests are blocked for 8 seconds"),
        ]

        async def run():
            with patch(
                "ariston_net_api.ariston_api.aiohttp.ClientSession",
                _session_factory(responses, sessions),
            ), patch(
                "ariston_net_api.ariston_api.asyncio.sleep", new_callable=AsyncMock
            ):
                with self.assertRaises(RateLimitException) as raised:
                    await AristonAPI("user", "pass")._async_get(
                        "https://example.test/state"
                    )
                self.assertEqual(raised.exception.retry_after, 10)

        asyncio.run(run())
        self.assertEqual(len(sessions), 2)

    def test_async_clients_for_same_account_are_serialized(self):
        active_requests = 0
        max_active_requests = 0

        class _Session:
            def __init__(self, *, timeout):
                self.timeout = timeout

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def request(self, *args, **kwargs):
                nonlocal active_requests, max_active_requests
                active_requests += 1
                max_active_requests = max(max_active_requests, active_requests)
                await asyncio.sleep(0)
                active_requests -= 1
                return _AsyncResponse(200, b"{}", {"ok": True})

        async def run():
            first = AristonAPI("same-user", "pass")
            second = AristonAPI("same-user", "pass")
            with patch(
                "ariston_net_api.ariston_api.aiohttp.ClientSession", _Session
            ), patch(
                "ariston_net_api.ariston_api._MIN_REQUEST_INTERVAL_SECONDS", 0
            ):
                return await asyncio.gather(
                    first._async_get("https://example.test/first"),
                    second._async_get("https://example.test/second"),
                )

        self.assertEqual(
            asyncio.run(run()), [{"ok": True}, {"ok": True}]
        )
        self.assertEqual(max_active_requests, 1)

    def test_async_unauthorized_requests_share_one_token_refresh(self):
        login_calls = 0
        unauthorized_calls = 0

        class _Session:
            def __init__(self, *, timeout):
                self.timeout = timeout

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def request(self, _method, path, **kwargs):
                nonlocal login_calls, unauthorized_calls
                if path.endswith(ARISTON_LOGIN):
                    login_calls += 1
                    return _AsyncResponse(200, b"{}", {"token": "fresh"})

                if kwargs["headers"]["ar.authToken"] == "stale":
                    unauthorized_calls += 1
                    return _AsyncResponse(405, b"invalid token")
                return _AsyncResponse(200, b"{}", {"ok": path})

        async def run():
            first = AristonAPI("same-user", "pass")
            second = AristonAPI("same-user", "pass")
            first._store_token("stale")
            with patch(
                "ariston_net_api.ariston_api.aiohttp.ClientSession", _Session
            ), patch(
                "ariston_net_api.ariston_api._MIN_REQUEST_INTERVAL_SECONDS", 0
            ):
                return await asyncio.gather(
                    first._async_get("https://example.test/first"),
                    second._async_get("https://example.test/second"),
                )

        self.assertEqual(
            asyncio.run(run()),
            [
                {"ok": "https://example.test/first"},
                {"ok": "https://example.test/second"},
            ],
        )
        self.assertEqual(login_calls, 1)
        self.assertEqual(unauthorized_calls, 1)

    def test_sync_and_async_clients_share_one_request_lock(self):
        async_started = threading.Event()
        release_async = threading.Event()
        sync_started = threading.Event()
        active_requests = 0
        max_active_requests = 0

        class _Session:
            def __init__(self, *, timeout):
                self.timeout = timeout

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def request(self, *args, **kwargs):
                nonlocal active_requests, max_active_requests
                active_requests += 1
                max_active_requests = max(max_active_requests, active_requests)
                async_started.set()
                await asyncio.to_thread(release_async.wait)
                active_requests -= 1
                return _AsyncResponse(200, b"{}", {"async": True})

        def sync_request(*args, **kwargs):
            nonlocal active_requests, max_active_requests
            active_requests += 1
            max_active_requests = max(max_active_requests, active_requests)
            sync_started.set()
            active_requests -= 1
            response = MagicMock(
                ok=True,
                status_code=200,
                content=b"{}",
                headers={},
            )
            response.json.return_value = {"sync": True}
            return response

        async def run():
            async_api = AristonAPI("same-user", "pass")
            sync_api = AristonAPI("same-user", "pass")
            with patch(
                "ariston_net_api.ariston_api.aiohttp.ClientSession", _Session
            ), patch(
                "ariston_net_api.ariston_api.requests.request", sync_request
            ), patch(
                "ariston_net_api.ariston_api._MIN_REQUEST_INTERVAL_SECONDS", 0
            ):
                async_task = asyncio.create_task(
                    async_api._async_get("https://example.test/async")
                )
                await asyncio.to_thread(async_started.wait, 1)
                sync_task = asyncio.create_task(
                    asyncio.to_thread(
                        sync_api._get, "https://example.test/sync"
                    )
                )
                await asyncio.sleep(0.05)
                self.assertFalse(sync_started.is_set())
                release_async.set()
                return await asyncio.gather(async_task, sync_task)

        self.assertEqual(
            asyncio.run(run()), [{"async": True}, {"sync": True}]
        )
        self.assertEqual(max_active_requests, 1)

    def test_galevo_empty_diagnostics_are_not_retried_every_poll(self):
        async def run():
            api = MagicMock()
            api.async_get_properties = AsyncMock(return_value={})
            api.async_get_menu_items = AsyncMock(return_value=[])
            device = AristonGalevoDevice(
                api,
                {
                    DeviceAttribute.GW: "nimbus-gateway",
                    DeviceAttribute.NAME: "Nimbus",
                },
            )
            device.features = {"loaded": True}
            with patch(
                "ariston_net_api.galevo_device.time.monotonic", return_value=100.0
            ), patch.object(AristonGalevoDevice, "_update_state"):
                await device.async_update_state()
                await device.async_update_state()
            return api.async_get_menu_items.await_count

        self.assertEqual(asyncio.run(run()), 1)

    def test_galevo_failed_diagnostics_are_not_retried_every_poll(self):
        async def run():
            api = MagicMock()
            api.async_get_properties = AsyncMock(return_value={})
            api.async_get_menu_items = AsyncMock(
                side_effect=ConnectionException(500)
            )
            device = AristonGalevoDevice(
                api,
                {
                    DeviceAttribute.GW: "nimbus-gateway",
                    DeviceAttribute.NAME: "Nimbus",
                },
            )
            device.features = {"loaded": True}
            with patch(
                "ariston_net_api.galevo_device.time.monotonic", return_value=100.0
            ), patch.object(AristonGalevoDevice, "_update_state"):
                await device.async_update_state()
                await device.async_update_state()
            return api.async_get_menu_items.await_count

        self.assertEqual(asyncio.run(run()), 1)
