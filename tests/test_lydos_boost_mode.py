"""Tests for Lydos Hybrid temporary BOOST mode transitions."""

import asyncio
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock

from ariston_net_api.const import LydosDeviceProperties, LydosPlantMode
from ariston_net_api.lydos_hybrid_device import AristonLydosHybridDevice


def _device() -> AristonLydosHybridDevice:
    return AristonLydosHybridDevice(MagicMock(), {})


def _cloud_device(*modes: object) -> AristonLydosHybridDevice:
    api = MagicMock()
    api.async_get_velis_plant_data = AsyncMock(
        side_effect=[{LydosDeviceProperties.MODE: mode} for mode in modes]
    )
    api.async_set_lydos_mode = AsyncMock()
    return AristonLydosHybridDevice(api, {})


class LydosBoostModeTests(TestCase):
    """Verify that temporary BOOST returns to the preceding stable mode."""

    def test_invalid_mode_after_boost_restores_each_stable_mode(self):
        for stable_mode in (
            LydosPlantMode.IMEMORY,
            LydosPlantMode.GREEN,
            LydosPlantMode.PROGRAM,
        ):
            with self.subTest(stable_mode=stable_mode.name):
                device = _device()

                device.data[LydosDeviceProperties.MODE] = stable_mode.value
                device._observe_cloud_operation_mode(stable_mode.value)
                self.assertEqual(
                    device.water_heater_current_mode_text, stable_mode.name
                )

                device.data[LydosDeviceProperties.MODE] = LydosPlantMode.BOOST.value
                device._observe_cloud_operation_mode(LydosPlantMode.BOOST.value)
                self.assertEqual(device.water_heater_current_mode_text, "BOOST")

                for unexpected_mode in (
                    0,
                    None,
                    -1,
                    99,
                    "UNKNOWN",
                    "other",
                    True,
                    False,
                    [],
                    {},
                ):
                    with self.subTest(unexpected_mode=unexpected_mode):
                        device.data[LydosDeviceProperties.MODE] = unexpected_mode
                        self.assertEqual(
                            device.water_heater_mode_value, stable_mode.value
                        )
                        self.assertEqual(
                            device.water_heater_current_mode_text, stable_mode.name
                        )

    def test_unknown_without_observed_boost_remains_unknown(self):
        device = _device()
        device.data[LydosDeviceProperties.MODE] = 0

        self.assertIsNone(device.water_heater_mode_value)
        self.assertEqual(device.water_heater_current_mode_text, "UNKNOWN")

    def test_next_valid_mode_replaces_boost_fallback(self):
        device = _device()
        device.data[LydosDeviceProperties.MODE] = LydosPlantMode.GREEN.value
        device._observe_cloud_operation_mode(LydosPlantMode.GREEN.value)
        self.assertEqual(device.water_heater_current_mode_text, "GREEN")

        device.data[LydosDeviceProperties.MODE] = LydosPlantMode.BOOST.value
        device._observe_cloud_operation_mode(LydosPlantMode.BOOST.value)
        self.assertEqual(device.water_heater_current_mode_text, "BOOST")

        device.data[LydosDeviceProperties.MODE] = 0
        self.assertEqual(device.water_heater_current_mode_text, "GREEN")

        device.data[LydosDeviceProperties.MODE] = LydosPlantMode.GREEN.value
        device._observe_cloud_operation_mode(LydosPlantMode.GREEN.value)
        self.assertEqual(device.water_heater_current_mode_text, "GREEN")

        device.data[LydosDeviceProperties.MODE] = LydosPlantMode.IMEMORY.value
        device._observe_cloud_operation_mode(LydosPlantMode.IMEMORY.value)
        self.assertEqual(device.water_heater_current_mode_text, "IMEMORY")

        device.data[LydosDeviceProperties.MODE] = 0
        self.assertEqual(device.water_heater_current_mode_text, "UNKNOWN")

    def test_async_boost_write_captures_current_mode_without_prior_read(self):
        async def run():
            device = _device()
            device.api.async_set_lydos_mode = AsyncMock()
            device.data[LydosDeviceProperties.MODE] = LydosPlantMode.PROGRAM.value

            await device.async_set_water_heater_operation_mode("BOOST")
            device.data[LydosDeviceProperties.MODE] = 0
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "PROGRAM")
        device.api.async_set_lydos_mode.assert_awaited_once_with(
            device.gw, LydosPlantMode.BOOST
        )

    def test_delayed_cloud_snapshot_does_not_discard_local_boost_context(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.GREEN.value,
                LydosPlantMode.GREEN.value,
                None,
            )

            await device.async_update_state()
            await device.async_set_water_heater_operation_mode("BOOST")
            await device.async_update_state()
            await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "GREEN")

    def test_app_boost_is_observed_without_intermediate_property_reads(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.PROGRAM.value,
                LydosPlantMode.BOOST.value,
                "unexpected-mode",
            )

            await device.async_update_state()
            await device.async_update_state()
            await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "PROGRAM")

    def test_different_valid_mode_after_boost_does_not_replace_expected_mode(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.IMEMORY.value,
                LydosPlantMode.BOOST.value,
                LydosPlantMode.GREEN.value,
            )

            await device.async_update_state()
            await device.async_update_state()
            await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "IMEMORY")

    def test_unobserved_local_boost_context_eventually_finishes(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.GREEN.value,
                LydosPlantMode.GREEN.value,
                None,
                LydosPlantMode.GREEN.value,
                LydosPlantMode.GREEN.value,
                LydosPlantMode.IMEMORY.value,
            )

            await device.async_update_state()
            await device.async_set_water_heater_operation_mode("BOOST")
            for _ in range(5):
                await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "IMEMORY")

    def test_starting_during_boost_learns_the_next_stable_mode(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.BOOST.value,
                LydosPlantMode.GREEN.value,
                LydosPlantMode.BOOST.value,
                None,
            )

            for _ in range(4):
                await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "GREEN")

    def test_interrupted_alternative_mode_is_not_treated_as_consecutive(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.PROGRAM.value,
                LydosPlantMode.BOOST.value,
                LydosPlantMode.GREEN.value,
                None,
                LydosPlantMode.GREEN.value,
                None,
            )

            states = []
            for _ in range(6):
                await device.async_update_state()
                states.append(device.water_heater_current_mode_text)
            return states

        states = asyncio.run(run())

        self.assertEqual(
            states,
            ["PROGRAM", "BOOST", "PROGRAM", "PROGRAM", "PROGRAM", "PROGRAM"],
        )

    def test_second_local_boost_preserves_the_protected_previous_mode(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.IMEMORY.value,
                LydosPlantMode.BOOST.value,
                LydosPlantMode.GREEN.value,
                None,
            )

            await device.async_update_state()
            await device.async_update_state()
            await device.async_update_state()
            await device.async_set_water_heater_operation_mode("BOOST")
            await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "IMEMORY")

    def test_repeated_different_valid_mode_is_accepted_as_external_change(self):
        async def run():
            device = _cloud_device(
                LydosPlantMode.PROGRAM.value,
                LydosPlantMode.BOOST.value,
                LydosPlantMode.GREEN.value,
                LydosPlantMode.GREEN.value,
            )

            for _ in range(4):
                await device.async_update_state()
            return device

        device = asyncio.run(run())

        self.assertEqual(device.water_heater_current_mode_text, "GREEN")
