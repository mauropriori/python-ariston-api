"""Lydos hybrid device class for Ariston module."""
from __future__ import annotations

import logging
from typing import Optional

from .const import (
    ConsumptionTimeInterval,
    ConsumptionType,
    LydosDeviceProperties,
    LydosPlantMode,
    PlantData,
    SeDeviceSettings,
)
from .evo_lydos_device import AristonEvoLydosDevice

_LOGGER = logging.getLogger(__name__)


class AristonLydosHybridDevice(AristonEvoLydosDevice):
    """Class representing a physical device, it's state and properties."""

    _STABLE_OPERATION_MODES = frozenset(
        {
            LydosPlantMode.IMEMORY.value,
            LydosPlantMode.GREEN.value,
            LydosPlantMode.PROGRAM.value,
        }
    )

    @classmethod
    def _stable_operation_mode(cls, mode: object) -> Optional[int]:
        """Return a recognized stable mode without accepting bool or bad JSON."""
        if type(mode) is int and mode in cls._STABLE_OPERATION_MODES:
            return mode
        return None

    @staticmethod
    def _is_boost_mode(mode: object) -> bool:
        """Return whether mode is exactly the Lydos BOOST integer code."""
        return type(mode) is int and mode == LydosPlantMode.BOOST.value

    def _reset_temporary_boost_tracking(self) -> None:
        """Finish a temporary BOOST transition without changing the device."""
        self._temporary_boost_seen = False
        self._cloud_boost_observed = False
        self._local_boost_pending = False
        self._expected_mode_poll_count = 0
        self._post_boost_candidate_mode = None
        self._post_boost_candidate_poll_count = 0

    def _start_local_temporary_boost(self, previous_mode: Optional[int]) -> None:
        """Track a successful local BOOST request until cloud convergence."""
        if previous_mode is not None:
            self._last_stable_operation_mode = previous_mode
        self._temporary_boost_seen = True
        self._cloud_boost_observed = False
        self._local_boost_pending = True
        self._expected_mode_poll_count = 0
        self._post_boost_candidate_mode = None
        self._post_boost_candidate_poll_count = 0

    def _observe_cloud_operation_mode(self, mode: object) -> None:
        """Remember cloud mode transitions without writing to the appliance."""
        stable_mode = self._stable_operation_mode(mode)
        if stable_mode is not None:
            boost_transition = getattr(
                self, "_temporary_boost_seen", False
            ) or getattr(self, "_local_boost_pending", False)
            if not boost_transition:
                self._last_stable_operation_mode = stable_mode
                return

            previous_mode = getattr(self, "_last_stable_operation_mode", None)
            if previous_mode is None:
                # HA may start while BOOST is already active. The first stable
                # cloud mode then becomes the baseline for future cycles.
                self._last_stable_operation_mode = stable_mode
                self._reset_temporary_boost_tracking()
                return

            if stable_mode == previous_mode:
                self._post_boost_candidate_mode = None
                self._post_boost_candidate_poll_count = 0
                if getattr(self, "_cloud_boost_observed", False):
                    self._reset_temporary_boost_tracking()
                    return

                # A locally requested BOOST can start and finish between two
                # polls. One old snapshot is tolerated; a second matching
                # snapshot confirms that the device is stable again.
                self._expected_mode_poll_count = (
                    getattr(self, "_expected_mode_poll_count", 0) + 1
                )
                if self._expected_mode_poll_count >= 2:
                    self._reset_temporary_boost_tracking()
                return

            # One recognized but unexpected mode can still be a stale/broken
            # post-BOOST response. Accept it only after two matching polls, so
            # a deliberate app-side mode change is not hidden indefinitely.
            self._expected_mode_poll_count = 0
            if getattr(self, "_post_boost_candidate_mode", None) == stable_mode:
                self._post_boost_candidate_poll_count = (
                    getattr(self, "_post_boost_candidate_poll_count", 0) + 1
                )
            else:
                self._post_boost_candidate_mode = stable_mode
                self._post_boost_candidate_poll_count = 1

            if self._post_boost_candidate_poll_count >= 2:
                self._last_stable_operation_mode = stable_mode
                self._reset_temporary_boost_tracking()
            return

        if self._is_boost_mode(mode):
            self._temporary_boost_seen = True
            self._cloud_boost_observed = True
            self._local_boost_pending = False
            self._expected_mode_poll_count = 0
            self._post_boost_candidate_mode = None
            self._post_boost_candidate_poll_count = 0
            return

        # Malformed snapshots do not end BOOST, but they interrupt any
        # consecutive stable-mode confirmation sequence.
        if getattr(self, "_temporary_boost_seen", False) or getattr(
            self, "_local_boost_pending", False
        ):
            self._expected_mode_poll_count = 0
            self._post_boost_candidate_mode = None
            self._post_boost_candidate_poll_count = 0

    def _resolved_operation_mode(self, mode: object) -> Optional[int]:
        """Resolve malformed post-BOOST values to the preceding stable mode."""
        stable_mode = self._stable_operation_mode(mode)
        if self._is_boost_mode(mode):
            return LydosPlantMode.BOOST.value

        boost_transition = getattr(self, "_temporary_boost_seen", False) or getattr(
            self, "_local_boost_pending", False
        )
        previous_mode = getattr(self, "_last_stable_operation_mode", None)
        if boost_transition and previous_mode is not None:
            if stable_mode != previous_mode:
                return previous_mode

        return stable_mode

    def update_state(self) -> None:
        """Update state and observe BOOST transitions at acquisition time."""
        super().update_state()
        self._observe_cloud_operation_mode(
            self.data.get(LydosDeviceProperties.MODE, None)
        )

    async def async_update_state(self) -> None:
        """Asynchronously update and observe BOOST transitions."""
        await super().async_update_state()
        self._observe_cloud_operation_mode(
            self.data.get(LydosDeviceProperties.MODE, None)
        )

    @property
    def water_heater_mode_value(self) -> Optional[int]:
        """Return a stable mode across the end of temporary BOOST."""
        return self._resolved_operation_mode(
            self.data.get(LydosDeviceProperties.MODE, None)
        )

    @property
    def plant_data(self) -> PlantData:
        """Final string to get plant data"""
        return PlantData.Se

    @property
    def anti_legionella_on_off(self) -> str:
        """Final string to get anti-legionella-on-off"""
        return SeDeviceSettings.SE_ANTILEGIONELLA_ON_OFF

    @property
    def consumption_type(self) -> str:
        """String to get consumption type"""
        return "DhwHeatingPumpElec%2CDhwResistorElec"

    @property
    def water_heater_mode(self) -> type[LydosPlantMode]:
        """Return the water heater mode class"""
        return LydosPlantMode

    @property
    def max_setpoint_temp(self) -> str:
        return SeDeviceSettings.SE_MAX_SETPOINT_TEMPERATURE

    @property
    def water_heater_maximum_setpoint_temperature_minimum(self) -> Optional[float]:
        """Get water heater maximum setpoint temperature minimum"""
        return self.plant_settings.get(
            SeDeviceSettings.SE_MAX_SETPOINT_TEMPERATURE_MIN, None
        )

    @property
    def water_heater_maximum_setpoint_temperature_maximum(self) -> Optional[float]:
        """Get water heater maximum setpoint maximum temperature"""
        return self.plant_settings.get(
            SeDeviceSettings.SE_MAX_SETPOINT_TEMPERATURE_MAX, None
        )

    @property
    def electric_consumption_for_water_last_two_hours(self) -> int:
        """Get electric consumption for water last value"""
        return self._get_consumption_sequence_last_value(
            ConsumptionType.DOMESTIC_HOT_WATER_HEATING_PUMP_ELECTRICITY,
            ConsumptionTimeInterval.LAST_DAY,
        )

    @property
    def permanent_boost_value(self) -> int:
        """Get permanent boost value"""
        return self.plant_settings.get(SeDeviceSettings.SE_PERMANENT_BOOST_ON_OFF, 0)

    @property
    def anti_cooling_value(self) -> int:
        """Get anti cooling value"""
        return self.plant_settings.get(SeDeviceSettings.SE_ANTI_COOLING_ON_OFF, 0)

    @property
    def anti_cooling_temperature_value(self) -> int:
        """Get anti cooling temperature value"""
        return self.plant_settings.get(SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE, 0)

    @property
    def anti_cooling_temperature_maximum(self) -> int:
        """Get anti cooling temperature maximum"""
        return self.plant_settings.get(SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE_MAX, 0)

    @property
    def anti_cooling_temperature_minimum(self) -> int:
        """Get anti cooling temperature minimum"""
        return self.plant_settings.get(SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE_MIN, 0)

    @property
    def night_mode_value(self) -> int:
        """Get night mode value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_MODE_ON_OFF, 0)

    @property
    def night_mode_begin_as_minutes_value(self) -> int:
        """Get night mode begin as minutes value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_BEGIN_AS_MINUTES, 0)

    @property
    def night_mode_begin_max_as_minutes_value(self) -> int:
        """Get night mode begin max as minutes value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_BEGIN_MAX_AS_MINUTES, 0)

    @property
    def night_mode_begin_min_as_minutes_value(self) -> int:
        """Get night mode begin min as minutes value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_BEGIN_MIN_AS_MINUTES, 0)

    @property
    def night_mode_end_as_minutes_value(self) -> int:
        """Get night mode end as minutes value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_END_AS_MINUTES, 0)

    @property
    def night_mode_end_max_as_minutes_value(self) -> int:
        """Get night mode end max as minutes value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_END_MAX_AS_MINUTES, 0)

    @property
    def night_mode_end_min_as_minutes_value(self) -> int:
        """Get night mode end min as minutes value"""
        return self.plant_settings.get(SeDeviceSettings.SE_NIGHT_END_MIN_AS_MINUTES, 0)

    def set_water_heater_operation_mode(self, operation_mode: str):
        """Set water heater operation mode"""
        requested_mode = LydosPlantMode[operation_mode]
        previous_mode = self._stable_operation_mode(
            self._resolved_operation_mode(
                self.data.get(LydosDeviceProperties.MODE, None)
            )
        )
        self.api.set_lydos_mode(self.gw, requested_mode)

        if requested_mode is LydosPlantMode.BOOST:
            self._start_local_temporary_boost(previous_mode)
        else:
            self._last_stable_operation_mode = requested_mode.value
            self._reset_temporary_boost_tracking()

        self.data[LydosDeviceProperties.MODE] = requested_mode.value

    async def async_set_water_heater_operation_mode(self, operation_mode: str):
        """Async set water heater operation mode"""
        requested_mode = LydosPlantMode[operation_mode]
        previous_mode = self._stable_operation_mode(
            self._resolved_operation_mode(
                self.data.get(LydosDeviceProperties.MODE, None)
            )
        )
        await self.api.async_set_lydos_mode(self.gw, requested_mode)

        if requested_mode is LydosPlantMode.BOOST:
            self._start_local_temporary_boost(previous_mode)
        else:
            self._last_stable_operation_mode = requested_mode.value
            self._reset_temporary_boost_tracking()

        self.data[LydosDeviceProperties.MODE] = requested_mode.value

    def set_water_heater_temperature(self, temperature: float):
        """Set water heater temperature"""
        self.api.set_lydos_temperature(self.gw, temperature)
        self.data[LydosDeviceProperties.REQ_TEMP] = temperature

    async def async_set_water_heater_temperature(self, temperature: float):
        """Async set water heater temperature"""
        await self.api.async_set_lydos_temperature(self.gw, temperature)
        self.data[LydosDeviceProperties.REQ_TEMP] = temperature

    def set_permanent_boost_value(self, boost: float) -> None:
        """Set permanent boost value"""
        self.api.set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_PERMANENT_BOOST_ON_OFF,
            1.0 if boost else 0.0,
            1.0 if self.permanent_boost_value else 0.0,
        )
        self.plant_settings[SeDeviceSettings.SE_PERMANENT_BOOST_ON_OFF] = boost

    async def async_set_permanent_boost_value(self, boost: float) -> None:
        """Async set permanent boost value"""
        await self.api.async_set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_PERMANENT_BOOST_ON_OFF,
            1.0 if boost else 0.0,
            1.0 if self.permanent_boost_value else 0.0,
        )
        self.plant_settings[SeDeviceSettings.SE_PERMANENT_BOOST_ON_OFF] = boost

    def set_anti_cooling_value(self, anti_cooling: float) -> None:
        """Set anti cooling value"""
        self.api.set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_ANTI_COOLING_ON_OFF,
            1.0 if anti_cooling else 0.0,
            1.0 if self.anti_cooling_value else 0.0,
        )
        self.plant_settings[SeDeviceSettings.SE_ANTI_COOLING_ON_OFF] = anti_cooling

    async def async_set_anti_cooling_value(self, anti_cooling: float) -> None:
        """Async set anti cooling value"""
        await self.api.async_set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_ANTI_COOLING_ON_OFF,
            1.0 if anti_cooling else 0.0,
            1.0 if self.anti_cooling_value else 0.0,
        )
        self.plant_settings[SeDeviceSettings.SE_ANTI_COOLING_ON_OFF] = anti_cooling

    def set_cooling_temperature_value(self, temperature: float) -> None:
        """Set cooling temperature value"""
        self.api.set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE,
            temperature,
            self.anti_cooling_temperature_value,
        )
        self.plant_settings[SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE] = temperature

    async def async_set_cooling_temperature_value(self, temperature: float) -> None:
        """Async set cooling temperature value"""
        await self.api.async_set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE,
            temperature,
            self.anti_cooling_temperature_value,
        )
        self.plant_settings[SeDeviceSettings.SE_ANTI_COOLING_TEMPERATURE] = temperature

    def set_night_mode_value(self, night_mode: float) -> None:
        """Set night mode value"""
        self.api.set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_NIGHT_MODE_ON_OFF,
            1.0 if night_mode else 0.0,
            1.0 if self.night_mode_value else 0.0,
        )
        self.plant_settings[SeDeviceSettings.SE_NIGHT_MODE_ON_OFF] = night_mode

    async def async_set_night_mode_value(self, night_mode: float) -> None:
        """Async set night mode value"""
        await self.api.async_set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_NIGHT_MODE_ON_OFF,
            1.0 if night_mode else 0.0,
            1.0 if self.night_mode_value else 0.0,
        )
        self.plant_settings[SeDeviceSettings.SE_NIGHT_MODE_ON_OFF] = night_mode

    def set_night_mode_begin_as_minutes_value(self, night_mode_begin_as_minutes: int) -> None:
        """Set night mode begin as minutes value"""
        self.api.set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_NIGHT_BEGIN_AS_MINUTES,
            night_mode_begin_as_minutes,
            self.night_mode_begin_as_minutes_value,
        )
        self.plant_settings[SeDeviceSettings.SE_NIGHT_BEGIN_AS_MINUTES] = night_mode_begin_as_minutes

    async def async_set_night_mode_begin_as_minutes_value(self, night_mode_begin_as_minutes: int) -> None:
        """Async set night mode begin as minutes value"""
        await self.api.async_set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_NIGHT_BEGIN_AS_MINUTES,
            night_mode_begin_as_minutes,
            self.night_mode_begin_as_minutes_value,
        )
        self.plant_settings[SeDeviceSettings.SE_NIGHT_BEGIN_AS_MINUTES] = night_mode_begin_as_minutes

    def set_night_mode_end_as_minutes_value(self, night_mode_end_as_minutes: int) -> None:
        """Set night mode end as minutes value"""
        self.api.set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_NIGHT_END_AS_MINUTES,
            night_mode_end_as_minutes,
            self.night_mode_end_as_minutes_value,
        )
        self.plant_settings[SeDeviceSettings.SE_NIGHT_END_AS_MINUTES] = night_mode_end_as_minutes

    async def async_set_night_mode_end_as_minutes_value(self, night_mode_end_as_minutes: int) -> None:
        """Async set night mode end as minutes value"""
        await self.api.async_set_velis_plant_setting(
            self.plant_data,
            self.gw,
            SeDeviceSettings.SE_NIGHT_END_AS_MINUTES,
            night_mode_end_as_minutes,
            self.night_mode_end_as_minutes_value,
        )
        self.plant_settings[SeDeviceSettings.SE_NIGHT_END_AS_MINUTES] = night_mode_end_as_minutes
