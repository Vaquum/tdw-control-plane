from __future__ import annotations

import importlib.util

REMOVED_ASSET_MODULES = (
    'cleanup_binance_daily_trades',
    'create_binance_agg_trades_table',
    'create_binance_daily_trades_table',
    'create_binance_futures_trades_table',
    'create_binance_trades_complete_view',
    'create_binance_trades_daily_summary',
    'create_binance_trades_day_of_month_summary',
    'create_binance_trades_hour_of_day_summary',
    'create_binance_trades_hourly_summary',
    'create_binance_trades_month_of_year_summary',
    'create_binance_trades_monthly_summary',
    'create_binance_trades_table',
    'create_binance_trades_week_of_year_summary',
    'create_tdw_database',
    'create_tdw_database_v2',
    'daily_trades_to_tdw',
    'monthly_agg_trades_to_tdw',
    'monthly_futures_agg_trades_to_tdw',
    'monthly_futures_trades_to_tdw',
    'monthly_trades_to_tdw',
)

FORBIDDEN_NAME_TOKENS = ('tdw', 'binance_trades_complete', '_summary')


def test_definitions_exposes_no_tdw_assets(origo_definitions_module: object) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    asset_keys: set[str] = set()
    for assets_def in defs.assets:
        asset_keys.update(key.to_user_string() for key in assets_def.keys)

    offending = {
        key for key in asset_keys if any(token in key for token in FORBIDDEN_NAME_TOKENS)
    }
    assert offending == set()


def test_definitions_exposes_no_tdw_jobs_or_schedules(
    origo_definitions_module: object,
) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    job_names = {job.name for job in defs.jobs}
    schedule_names = {schedule.name for schedule in defs.schedules}

    offending = {
        name
        for name in job_names | schedule_names
        if any(token in name for token in FORBIDDEN_NAME_TOKENS)
    }
    assert offending == set()


def test_tdw_asset_modules_are_absent() -> None:
    present = [
        name
        for name in REMOVED_ASSET_MODULES
        if importlib.util.find_spec(f'tdw_control_plane.assets.{name}') is not None
    ]
    assert present == []
    assert importlib.util.find_spec('tdw_control_plane.query.get_binance_spot_klines') is None
