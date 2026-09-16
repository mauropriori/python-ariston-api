"""Tests for cloud request throttling and failure handling."""

import asyncio
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from ariston.ariston_api import (
    AristonAPI,
    ConnectionException,
    RateLimitException,
    _parse_retry_after_seconds,
)


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
        AristonAPI._sync_locks.clear()
        AristonAPI._async_locks.clear()

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

    @patch("ariston.ariston_api.time.sleep")
    @patch("ariston.ariston_api.requests.request")
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

    @patch("ariston.ariston_api.time.sleep")
    @patch("ariston.ariston_api.requests.request")
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
                "ariston.ariston_api.aiohttp.ClientSession",
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
                "ariston.ariston_api.aiohttp.ClientSession",
                _session_factory(responses, sessions),
            ), patch(
                "ariston.ariston_api.asyncio.sleep", new_callable=AsyncMock
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
                "ariston.ariston_api.aiohttp.ClientSession",
                _session_factory(responses, sessions),
            ), patch("ariston.ariston_api.asyncio.sleep", new_callable=AsyncMock):
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
                "ariston.ariston_api.aiohttp.ClientSession", _Session
            ), patch("ariston.ariston_api._MIN_REQUEST_INTERVAL_SECONDS", 0):
                return await asyncio.gather(
                    first._async_get("https://example.test/first"),
                    second._async_get("https://example.test/second"),
                )

        self.assertEqual(
            asyncio.run(run()), [{"ok": True}, {"ok": True}]
        )
        self.assertEqual(max_active_requests, 1)
