"""Ariston API"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import threading
import time
from typing import Any, ClassVar, Optional
from weakref import WeakKeyDictionary

import aiohttp
import requests

from .const import (
    ARISTON_API_URL,
    ARISTON_BSB_ZONES,
    ARISTON_BUS_ERRORS,
    ARISTON_DATA_ITEMS,
    ARISTON_LITE,
    ARISTON_LOGIN,
    ARISTON_MENU_ITEMS,
    ARISTON_PLANTS,
    ARISTON_REMOTE,
    ARISTON_REPORTS,
    ARISTON_TIME_PROGS,
    ARISTON_USER_AGENT,
    ARISTON_VELIS,
    BsbOperativeMode,
    BsbZoneMode,
    DeviceFeatures,
    DeviceProperties,
    LydosPlantMode,
    MenuItemNames,
    NuosSplitOperativeMode,
    PlantData,
    ThermostatProperties,
    WaterHeaterMode,
    ZoneAttribute,
)

_LOGGER = logging.getLogger(__name__)

_RATE_LIMIT_RE = re.compile(rb"blocked for (\d+) seconds", re.IGNORECASE)
_DEFAULT_RATE_LIMIT_SECONDS = 60
_RATE_LIMIT_SAFETY_MARGIN_SECONDS = 2
_MIN_REQUEST_INTERVAL_SECONDS = 1.0
_REQUEST_TIMEOUT_SECONDS = 30


def _parse_retry_after_seconds(headers: Any, content: bytes) -> int:
    """Return the server-requested pause for a HTTP 429 response."""
    retry_after = None
    if headers is not None:
        retry_after = headers.get("Retry-After")

    if retry_after is not None:
        try:
            return max(0, math.ceil(float(retry_after)))
        except (TypeError, ValueError):
            _LOGGER.debug("Ignoring non-numeric Retry-After header: %s", retry_after)

    match = _RATE_LIMIT_RE.search(content)
    if match is not None:
        return int(match.group(1))

    return _DEFAULT_RATE_LIMIT_SECONDS


class _RequestState:
    """Rate-limit state shared by clients using the same cloud account."""

    def __init__(self) -> None:
        self.blocked_until = 0.0
        self.last_request_at = 0.0


class ConnectionException(Exception):
    """When can not connect to Ariston cloud"""


class RateLimitException(ConnectionException):
    """Raised when the Ariston cloud still rejects requests after backoff."""

    def __init__(self, retry_after: int, content: bytes = b"") -> None:
        self.retry_after = retry_after
        super().__init__(429, content)


class AristonAPI:
    """Ariston API class"""

    _request_states: ClassVar[dict[str, _RequestState]] = {}
    _sync_locks: ClassVar[dict[str, threading.Lock]] = {}
    _async_locks: ClassVar[WeakKeyDictionary] = WeakKeyDictionary()

    def __init__(
        self,
        username: str,
        password: str,
        api_url: str = ARISTON_API_URL,
        user_agent: str = ARISTON_USER_AGENT,
    ) -> None:
        """Constructor for Ariston API."""
        self.__username = username
        self.__password = password
        self.__api_url = api_url
        self.__token = ""
        self.__user_agent = user_agent
        self.__request_key = f"{api_url}|{username.strip().casefold()}"

    @classmethod
    def _get_request_state(cls, key: str) -> _RequestState:
        """Get shared request state for one account and API endpoint."""
        if key not in cls._request_states:
            cls._request_states[key] = _RequestState()
        return cls._request_states[key]

    @classmethod
    def _get_sync_lock(cls, key: str) -> threading.Lock:
        """Get a request lock shared by synchronous clients for one account."""
        if key not in cls._sync_locks:
            cls._sync_locks[key] = threading.Lock()
        return cls._sync_locks[key]

    @classmethod
    def _get_async_lock(cls, key: str) -> asyncio.Lock:
        """Get a request lock shared by async clients on the current event loop."""
        loop = asyncio.get_running_loop()
        locks = cls._async_locks.setdefault(loop, {})
        if key not in locks:
            locks[key] = asyncio.Lock()
        return locks[key]

    def _request_wait_seconds(self) -> float:
        """Calculate the pause needed before the next cloud request."""
        state = self._get_request_state(self.__request_key)
        now = time.monotonic()
        return max(
            0.0,
            state.blocked_until - now,
            state.last_request_at + _MIN_REQUEST_INTERVAL_SECONDS - now,
        )

    def _mark_request_complete(self) -> None:
        """Record completion time to keep subsequent requests spaced apart."""
        self._get_request_state(self.__request_key).last_request_at = time.monotonic()

    def _set_rate_limit(self, retry_after: int) -> int:
        """Pause every client for this account until the block expires."""
        wait_seconds = retry_after + _RATE_LIMIT_SAFETY_MARGIN_SECONDS
        state = self._get_request_state(self.__request_key)
        state.blocked_until = max(
            state.blocked_until, time.monotonic() + wait_seconds
        )
        _LOGGER.warning(
            "Ariston cloud rate limit reached; pausing this account for %s seconds",
            wait_seconds,
        )
        return wait_seconds

    def connect(self) -> bool:
        """Login to ariston cloud and get token"""

        try:
            response = self._post(
                f"{self.__api_url}{ARISTON_LOGIN}",
                {"usr": self.__username, "pwd": self.__password},
            )

            if response is None:
                return False

            self.__token = response["token"]

            return True

        except Exception as error:
            raise ConnectionException() from error

    def get_detailed_devices(self) -> list[Any]:
        """Get detailed cloud devices"""
        devices = self._get(f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}")
        if devices is not None:
            return list(devices)
        return list()

    def get_detailed_velis_devices(self) -> list[Any]:
        """Get detailed cloud devices"""
        devices = self._get(f"{self.__api_url}{ARISTON_VELIS}/{ARISTON_PLANTS}")
        if devices is not None:
            return list(devices)
        return list()

    def get_devices(self) -> list[Any]:
        """Get cloud devices"""
        devices = self._get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{ARISTON_LITE}"
        )
        if devices is not None:
            return list(devices)
        return list()

    def get_features_for_device(self, gw_id: str) -> dict[str, Any]:
        """Get features for the device"""
        features = self._get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{gw_id}/features"
        )
        if features is not None:
            return features
        return dict()

    def get_energy_account(self, gw_id: str) -> dict[str, Any]:
        """Get energy account for the device"""
        energy_account = self._get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_REPORTS}/{gw_id}/energyAccount"
        )
        if energy_account is not None:
            return energy_account
        return dict()

    def get_consumptions_sequences(self, gw_id: str, usages: str) -> list[Any]:
        """Get consumption sequences for the device"""
        consumptions_sequences = self._get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_REPORTS}/{gw_id}/consSequencesApi8?usages={usages}"
        )
        if consumptions_sequences is not None:
            return list(consumptions_sequences)
        return list()

    def get_consumptions_settings(self, gw_id: str) -> dict[str, Any]:
        """Get consumption settings"""
        consumptions_settings = self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{gw_id}/getConsumptionsSettings",
            {},
        )
        if consumptions_settings is not None:
            return consumptions_settings
        return dict()

    def set_consumptions_settings(
        self,
        gw_id: str,
        consumptions_settings: dict[str, Any],
    ) -> None:
        """Get consumption settings"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{gw_id}/consumptionsSettings",
            consumptions_settings,
        )

    @staticmethod
    def get_items(features: dict[str, Any]) -> list[dict[str, int]]:
        """Get the Final[str] strings from DeviceProperies and ThermostatProperties"""
        items: list[dict[str, Any]] = DeviceProperties.list(0)
        for zone in features[DeviceFeatures.ZONES]:
            items.extend(ThermostatProperties.list(zone[ZoneAttribute.NUM]))
        return items

    def get_properties(
        self, gw_id: str, features: dict[str, Any], culture: str, umsys: str
    ) -> dict[str, Any]:
        """Get device properties"""
        properties = self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_DATA_ITEMS}/{gw_id}/get?umsys={umsys}",
            {
                "useCache": False,
                "items": self.get_items(features),
                "features": features,
                "culture": culture,
            },
        )
        if properties is not None:
            return properties
        return dict()

    def get_bsb_plant_data(self, gw_id: str) -> dict[str, Any]:
        """Get BSB plant data."""
        data = self._get(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.Bsb.value}/{gw_id}"
        )
        if data is not None:
            return data
        return dict()

    def get_velis_plant_data(self, plant_data: PlantData, gw_id: str) -> dict[str, Any]:
        """Get Velis properties"""
        data = self._get(f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}")
        if data is not None:
            return data
        return dict()

    def get_velis_plant_settings(
        self, plant_data: PlantData, gw_id: str
    ) -> dict[str, Any]:
        """Get Velis settings"""
        settings = self._get(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}/plantSettings"
        )
        if settings is not None:
            return settings
        return dict()

    def get_menu_items(self, gw_id: str) -> list[dict[str, Any]]:
        """Get menu items"""
        items = self._get(
            f"{self.__api_url}{ARISTON_MENU_ITEMS}/{gw_id}?menuItems={MenuItemNames.values()}"
        )
        if items is not None:
            return items
        return list()

    def set_property(
        self,
        gw_id: str,
        zone_id: int,
        features: dict[str, Any],
        device_property: str,
        value: float,
        prev_value: float,
        umsys: str,
    ) -> None:
        """Set device properties"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_DATA_ITEMS}/{gw_id}/set?umsys={umsys}",
            {
                "items": [
                    {
                        "id": device_property,
                        "prevValue": prev_value,
                        "value": value,
                        "zone": zone_id,
                    }
                ],
                "features": features,
            },
        )

    def set_evo_number_of_showers(self, gw_id: str, number_of_showers: int) -> None:
        """Set Velis Evo number of showers"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.PD.value}/{gw_id}/showers",
            {
                "new": int(number_of_showers),
            },
        )

    def set_evo_mode(self, gw_id: str, value: WaterHeaterMode) -> None:
        """Set Velis Evo mode"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/mode",
            {
                "new": value.value,
            },
        )

    def set_lydos_mode(self, gw_id: str, value: LydosPlantMode) -> None:
        """Set Velis Lydos mode"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Se.value}/{gw_id}/mode",
            {
                "new": value.value,
            },
        )

    def set_nuos_mode(self, gw_id: str, value: NuosSplitOperativeMode) -> None:
        """Set Velis Nuos mode"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Slp.value}/{gw_id}/operativeMode",
            {
                "new": value.value,
            },
        )

    def set_bsb_mode(self, gw_id: str, value: BsbOperativeMode) -> None:
        """Set Bsb mode"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.Bsb.value}/{gw_id}/dhwMode",
            {
                "new": value.value,
            },
        )

    def set_bsb_zone_mode(
        self,
        gw_id: str,
        zone: int,
        value: BsbZoneMode,
        old_value: BsbZoneMode,
        is_cooling: bool,
    ) -> None:
        """Set Bsb zone mode"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_BSB_ZONES}/{gw_id}/{zone}/mode?isCooling={is_cooling}",
            {"new": value.value, "old": old_value.value},
        )

    def set_evo_temperature(self, gw_id: str, value: float) -> None:
        """Set Velis Evo temperature"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/temperature",
            {
                "new": value,
            },
        )

    def set_lydos_temperature(self, gw_id: str, value: float) -> None:
        """Set Velis Lydos temperature"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Se.value}/{gw_id}/temperature",
            {
                "new": value,
            },
        )

    def set_nuos_temperature(
        self,
        gw_id: str,
        comfort: float,
        reduced: float,
        old_comfort: Optional[float],
        old_reduced: Optional[float],
    ) -> None:
        """Set Nuos temperature"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Slp.value}/{gw_id}/temperatures",
            {
                "new": {
                    "comfort": comfort,
                    "reduced": reduced,
                },
                "old": {
                    "comfort": old_comfort,
                    "reduced": old_reduced,
                },
            },
        )

    def set_bsb_temperature(
        self,
        gw_id: str,
        comfort: float,
        reduced: float,
        old_comfort: Optional[float],
        old_reduced: Optional[float],
    ) -> None:
        """Set Bsb temperature"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.Bsb.value}/{gw_id}/dhwTemp",
            {
                "new": {
                    "comf": comfort,
                    "econ": reduced,
                },
                "old": {
                    "comf": old_comfort,
                    "econ": old_reduced,
                },
            },
        )

    def set_bsb_zone_temperature(
        self,
        gw_id: str,
        zone: int,
        comfort: float,
        reduced: float,
        old_comfort: Optional[float],
        old_reduced: Optional[float],
        is_cooling: bool,
    ) -> None:
        """Set Bsb zone temperature"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_BSB_ZONES}/{gw_id}/{zone}/temperatures?isCooling={is_cooling}",
            {
                "new": {
                    "comf": comfort,
                    "econ": reduced,
                },
                "old": {
                    "comf": old_comfort,
                    "econ": old_reduced,
                },
            },
        )

    def set_nous_boost(self, gw_id: str, boost: bool) -> None:
        """ "Set Nous boost"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Slp.value}/{gw_id}/boost",
            boost,
        )

    def set_evo_eco_mode(self, gw_id: str, eco_mode: bool) -> None:
        """Set Velis Evo eco mode"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/switchEco",
            eco_mode,
        )

    def set_lux_power_option(self, gw_id: str, power_option: bool) -> None:
        """Set Velis Lux2 power option"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/switchPowerOption",
            power_option,
        )

    def set_velis_power(self, plant_data: PlantData, gw_id: str, power: bool) -> None:
        """Set Velis power"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}/switch",
            power,
        )

    def set_velis_plant_setting(
        self,
        plant_data: PlantData,
        gw_id: str,
        setting: str,
        value: float,
        old_value: float,
    ) -> None:
        """Set Velis plant setting"""
        self._post(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}/plantSettings",
            {setting: {"new": value, "old": old_value}},
        )

    def get_thermostat_time_progs(
        self, gw_id: str, zone: int, umsys: str
    ) -> dict[str, Any]:
        """Get thermostat time programs"""
        thermostat_time_progs = self._get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_TIME_PROGS}/{gw_id}/ChZn{zone}?umsys={umsys}",
        )
        if thermostat_time_progs is not None:
            return thermostat_time_progs
        return dict()

    def set_holiday(
        self,
        gw_id: str,
        holiday_end_date: Optional[str],
    ) -> None:
        """Set holidays"""
        self._post(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.PD}/{gw_id}/holiday",
            {
                "new": holiday_end_date,
            },
        )

    def get_bus_errors(self, gw_id: str) -> list[Any]:
        """Get bus errors"""
        bus_errors = self._get(
            f"{self.__api_url}{ARISTON_BUS_ERRORS}?gatewayId={gw_id}&blockingOnly=False&culture=en-US"
        )
        if bus_errors is not None:
            return list(bus_errors)
        return []

    def __request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Any = None,
        is_retry: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Request with requests"""
        headers: dict[str, Any] = {
            "User-Agent": self.__user_agent,
            "ar.authToken": self.__token,
        }

        _LOGGER.debug(
            "Request method %s, path: %s, params: %s",
            method,
            path,
            params,
        )
        with self._get_sync_lock(self.__request_key):
            wait_seconds = self._request_wait_seconds()
            if wait_seconds > 0:
                time.sleep(wait_seconds)

            response = requests.request(
                method,
                path,
                params=params,
                json=body,
                headers=headers,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            self._mark_request_complete()

        if not response.ok:
            match response.status_code:
                case 405:
                    if not is_retry:
                        if self.connect():
                            return self.__request(method, path, params, body, True)
                        raise ConnectionException("Login failed (password changed?)")
                    raise ConnectionException("Invalid token")
                case 404:
                    return None
                case 429:
                    content = response.content
                    wait_seconds = self._set_rate_limit(
                        _parse_retry_after_seconds(response.headers, content)
                    )
                    if not is_retry:
                        time.sleep(wait_seconds)
                        return self.__request(method, path, params, body, True)
                    raise RateLimitException(wait_seconds, content)
                case _:
                    raise ConnectionException(response.status_code)

        if len(response.content) > 0:
            json = response.json()
            _LOGGER.debug("Response %s", json)
            return json

        return None

    def _post(self, path: str, body: Any) -> Any:
        """POST request"""
        return self.__request("POST", path, None, body)

    def _get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        """GET request"""
        return self.__request("GET", path, params, None)

    async def async_connect(self) -> bool:
        """Async login to ariston cloud and get token"""

        try:
            response = await self._async_post(
                f"{self.__api_url}{ARISTON_LOGIN}",
                {"usr": self.__username, "pwd": self.__password},
            )

            if response is None:
                return False

            self.__token = response["token"]

            return True

        except Exception as error:
            raise ConnectionException() from error

    async def async_get_detailed_devices(self) -> list[Any]:
        """Async get detailed cloud devices"""
        detailed_devices = await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}"
        )
        if detailed_devices is not None:
            return list(detailed_devices)
        return list()

    async def async_get_detailed_velis_devices(self) -> list[Any]:
        """Async get detailed cloud devices"""
        detailed_velis_devices = await self._async_get(
            f"{self.__api_url}{ARISTON_VELIS}/{ARISTON_PLANTS}"
        )
        if detailed_velis_devices is not None:
            return list(detailed_velis_devices)
        return list()

    async def async_get_devices(self) -> list[Any]:
        """Async get cloud devices"""
        devices = await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{ARISTON_LITE}"
        )
        if devices is not None:
            return list(devices)
        return list()

    async def async_get_features_for_device(
        self, gw_id: str
    ) -> Optional[dict[str, Any]]:
        """Async get features for the device"""
        return await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{gw_id}/features"
        )

    async def async_get_energy_account(self, gw_id: str) -> dict[str, Any]:
        """Async get energy account for the device"""
        energy_account = await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_REPORTS}/{gw_id}/energyAccount"
        )
        if energy_account is not None:
            return energy_account
        return dict()

    async def async_get_consumptions_sequences(
        self, gw_id: str, usages: str
    ) -> list[Any]:
        """Async get consumption sequences for the device"""
        consumptions_sequences = await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_REPORTS}/{gw_id}/consSequencesApi8?usages={usages}"
        )
        if consumptions_sequences is not None:
            return list(consumptions_sequences)
        return list()

    async def async_get_consumptions_settings(self, gw_id: str) -> dict[str, Any]:
        """Async get consumption settings"""
        consumptions_settings = await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{gw_id}/getConsumptionsSettings",
            {},
        )
        if consumptions_settings is not None:
            return consumptions_settings
        return dict()

    async def async_set_consumptions_settings(
        self,
        gw_id: str,
        consumptions_settings: dict[str, Any],
    ) -> None:
        """Async set consumption settings"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_PLANTS}/{gw_id}/consumptionsSettings",
            consumptions_settings,
        )

    async def async_get_properties(
        self, gw_id: str, features: dict[str, Any], culture: str, umsys: str
    ) -> dict[str, Any]:
        """Async get device properties"""
        properties = await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_DATA_ITEMS}/{gw_id}/get?umsys={umsys}",
            {
                "useCache": False,
                "items": self.get_items(features),
                "features": features,
                "culture": culture,
            },
        )
        if properties is not None:
            return properties
        return dict()

    async def async_get_bsb_plant_data(self, gw_id: str) -> dict[str, Any]:
        """Get BSB plant data."""
        data = await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.Bsb.value}/{gw_id}"
        )
        if data is not None:
            return data
        return dict()

    async def async_get_velis_plant_data(
        self, plant_data: PlantData, gw_id: str
    ) -> dict[str, Any]:
        """Async get Velis properties"""
        med_plant_data = await self._async_get(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}"
        )
        if med_plant_data is not None:
            return med_plant_data
        return dict()

    async def async_get_velis_plant_settings(
        self, plant_data: PlantData, gw_id: str
    ) -> dict[str, Any]:
        """Async get Velis settings"""
        med_plant_settings = await self._async_get(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}/plantSettings"
        )
        if med_plant_settings is not None:
            return med_plant_settings
        return dict()

    async def async_get_menu_items(self, gw_id: str) -> list[dict[str, Any]]:
        """Async get menu items"""
        items = await self._async_get(
            f"{self.__api_url}{ARISTON_MENU_ITEMS}/{gw_id}?menuItems={MenuItemNames.values()}"
        )
        if items is not None:
            return items
        return list()

    async def async_set_property(
        self,
        gw_id: str,
        zone_id: int,
        features: dict[str, Any],
        device_property: str,
        value: float,
        prev_value: float,
        umsys: str,
    ) -> None:
        """Async set device properties"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_DATA_ITEMS}/{gw_id}/set?umsys={umsys}",
            {
                "items": [
                    {
                        "id": device_property,
                        "prevValue": prev_value,
                        "value": value,
                        "zone": zone_id,
                    }
                ],
                "features": features,
            },
        )

    async def async_set_evo_number_of_showers(
        self, gw_id: str, number_of_showers: int
    ) -> None:
        """Set Velis Evo number of showers"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.PD.value}/{gw_id}/showers",
            {
                "new": int(number_of_showers),
            },
        )

    async def async_set_evo_mode(self, gw_id: str, value: WaterHeaterMode) -> None:
        """Async set Velis Evo mode"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/mode",
            {
                "new": value.value,
            },
        )

    async def async_set_lydos_mode(self, gw_id: str, value: LydosPlantMode) -> None:
        """Async set Velis Lydos mode"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Se.value}/{gw_id}/mode",
            {
                "new": value.value,
            },
        )

    async def async_set_nuos_mode(
        self, gw_id: str, value: NuosSplitOperativeMode
    ) -> None:
        """Async set Velis Nuos mode"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Slp.value}/{gw_id}/operativeMode",
            {
                "new": value.value,
            },
        )

    async def async_set_bsb_mode(self, gw_id: str, value: BsbOperativeMode) -> None:
        """Async set Bsb mode"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.Bsb.value}/{gw_id}/dhwMode",
            {
                "new": value.value,
            },
        )

    async def async_set_bsb_zone_mode(
        self,
        gw_id: str,
        zone: int,
        value: BsbZoneMode,
        old_value: BsbZoneMode,
        is_cooling: bool,
    ) -> None:
        """Async set Bsb zone mode"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_BSB_ZONES}/{gw_id}/{zone}/mode?isCooling={is_cooling}",
            {"new": value.value, "old": old_value.value},
        )

    async def async_set_evo_temperature(self, gw_id: str, value: float) -> None:
        """Async set Velis Evo temperature"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/temperature",
            {
                "new": value,
            },
        )

    async def async_set_lydos_temperature(self, gw_id: str, value: float) -> None:
        """Async set Velis Lydos temperature"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Se.value}/{gw_id}/temperature",
            {
                "new": value,
            },
        )

    async def async_set_nuos_temperature(
        self,
        gw_id: str,
        comfort: float,
        reduced: float,
        old_comfort: Optional[float],
        old_reduced: Optional[float],
    ) -> None:
        """Async set Velis Lydos temperature"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Slp.value}/{gw_id}/temperatures",
            {
                "new": {
                    "comfort": comfort,
                    "reduced": reduced,
                },
                "old": {
                    "comfort": old_comfort,
                    "reduced": old_reduced,
                },
            },
        )

    async def async_set_bsb_temperature(
        self,
        gw_id: str,
        comfort: float,
        reduced: float,
        old_comfort: Optional[float],
        old_reduced: Optional[float],
    ) -> None:
        """Async set Bsb temperature"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.Bsb.value}/{gw_id}/dhwTemp",
            {
                "new": {
                    "comf": comfort,
                    "econ": reduced,
                },
                "old": {
                    "comf": old_comfort,
                    "econ": old_reduced,
                },
            },
        )

    async def async_set_bsb_zone_temperature(
        self,
        gw_id: str,
        zone: int,
        comfort: float,
        reduced: float,
        old_comfort: Optional[float],
        old_reduced: Optional[float],
        is_cooling: bool,
    ) -> None:
        """Async set Bsb zone temperature"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_BSB_ZONES}/{gw_id}/{zone}/temperatures?isCooling={is_cooling}",
            {
                "new": {
                    "comf": comfort,
                    "econ": reduced,
                },
                "old": {
                    "comf": old_comfort,
                    "econ": old_reduced,
                },
            },
        )

    async def async_set_nous_boost(self, gw_id: str, boost: bool) -> None:
        """ "Set Nous boost"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Slp.value}/{gw_id}/boost",
            boost,
        )

    async def async_set_evo_eco_mode(self, gw_id: str, eco_mode: bool) -> None:
        """Async set Velis Evo eco mode"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/switchEco",
            eco_mode,
        )

    async def async_set_lux_power_option(self, gw_id: str, power_option: bool) -> None:
        """Set Velis Lux2 power option"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{PlantData.Med.value}/{gw_id}/switchPowerOption",
            power_option,
        )

    async def async_set_velis_power(
        self, plant_data: PlantData, gw_id: str, power: bool
    ) -> None:
        """Async set Velis power"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}/switch",
            power,
        )

    async def async_set_velis_plant_setting(
        self,
        plant_data: PlantData,
        gw_id: str,
        setting: str,
        value: float,
        old_value: float,
    ) -> None:
        """Async set Velis Evo plant setting"""
        await self._async_post(
            f"{self.__api_url}{ARISTON_VELIS}/{plant_data.value}/{gw_id}/plantSettings",
            {setting: {"new": value, "old": old_value}},
        )

    async def async_get_thermostat_time_progs(
        self, gw_id: str, zone: int, umsys: str
    ) -> Optional[dict[str, Any]]:
        """Async get thermostat time programs"""
        return await self._async_get(
            f"{self.__api_url}{ARISTON_REMOTE}/{ARISTON_TIME_PROGS}/{gw_id}/ChZn{zone}?umsys={umsys}",
        )

    async def async_set_holiday(
        self,
        gw_id: str,
        holiday_end_date: Optional[str],
    ) -> None:
        """Async set holidays"""

        await self._async_post(
            f"{self.__api_url}{ARISTON_REMOTE}/{PlantData.PD}/{gw_id}/holiday",
            {
                "new": holiday_end_date,
            },
        )

    async def async_get_bus_errors(self, gw_id: str) -> list[Any]:
        """Async get bus errors"""
        bus_errors = await self._async_get(
            f"{self.__api_url}{ARISTON_BUS_ERRORS}?gatewayId={gw_id}&blockingOnly=False&culture=en-US"
        )
        if bus_errors is not None:
            return list(bus_errors)
        return []

    async def __async_request(
        self,
        method: str,
        path: str,
        params: Optional[dict[str, Any]] = None,
        body: Any = None,
        is_retry: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Async request with aiohttp"""
        headers: dict[str, Any] = {
            "User-Agent": self.__user_agent,
            "ar.authToken": self.__token,
        }

        _LOGGER.debug(
            "Request method %s, path: %s, params: %s",
            method,
            path,
            params,
        )

        async with self._get_async_lock(self.__request_key):
            wait_seconds = self._request_wait_seconds()
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                response = await session.request(
                    method, path, params=params, json=body, headers=headers
                )
                content = await response.read()
                response_json = await response.json() if response.ok and content else None
                response_ok = response.ok
                response_status = response.status
                response_headers = response.headers
                self._mark_request_complete()

        if not response_ok:
            match response_status:
                case 405:
                    if not is_retry:
                        if await self.async_connect():
                            return await self.__async_request(
                                method, path, params, body, True
                            )
                        raise ConnectionException("Login failed (password changed?)")
                    raise ConnectionException("Invalid token")
                case 404:
                    return None
                case 429:
                    wait_seconds = self._set_rate_limit(
                        _parse_retry_after_seconds(response_headers, content)
                    )
                    if not is_retry:
                        await asyncio.sleep(wait_seconds)
                        return await self.__async_request(
                            method, path, params, body, True
                        )
                    raise RateLimitException(wait_seconds, content)
                case _:
                    raise ConnectionException(response_status)

        if response_json is not None:
            _LOGGER.debug("Response %s", response_json)
            return response_json

        return None

    async def _async_post(self, path: str, body: Any) -> Any:
        """Async POST request"""
        return await self.__async_request("POST", path, None, body)

    async def _async_get(
        self, path: str, params: Optional[dict[str, Any]] = None
    ) -> Any:
        """Async GET request"""
        return await self.__async_request("GET", path, params, None)
