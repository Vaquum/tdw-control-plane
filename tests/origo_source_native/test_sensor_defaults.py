from __future__ import annotations

from dagster import DefaultSensorStatus


def test_all_sensors_default_to_running(origo_definitions_module: object) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    not_running = {
        sensor.name
        for sensor in defs.sensors
        if sensor.default_status is not DefaultSensorStatus.RUNNING
    }
    assert not_running == set()
