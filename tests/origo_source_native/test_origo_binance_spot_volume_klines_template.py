from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import pytest
from dagster import DefaultScheduleStatus

from .helpers import ORIGO_DATABASE

VOLUME_KLINE_COLUMNS = [
    'start_datetime',
    'end_datetime',
    'volume_bar_id',
    'open',
    'high',
    'low',
    'close',
    'mean',
    'std',
    'median',
    'iqr',
    'volume',
    'maker_ratio',
    'no_of_trades',
    'open_liquidity',
    'high_liquidity',
    'low_liquidity',
    'close_liquidity',
    'liquidity_sum',
    'maker_volume',
    'maker_liquidity',
]
VOLUME_KLINE_COLUMN_TYPES = [
    'DateTime',
    'DateTime',
    'UInt64',
    *(['Float64'] * 10),
    'UInt64',
    *(['Float64'] * 7),
]


def _table_metadata(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    table_name: str,
) -> tuple[str, str, str]:
    rows = query_origo(
        f"""
        SELECT engine, partition_key, sorting_key
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name = '{table_name}'
        """
    )

    assert len(rows) == 1
    engine, partition_key, sorting_key = rows[0]
    return str(engine), str(partition_key), str(sorting_key)


def _normalize_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return value


def _rows_to_dicts(
    columns: list[str],
    rows: list[tuple[object, ...]],
) -> list[dict[str, object]]:
    return [
        {column: _normalize_value(value) for column, value in zip(columns, row, strict=True)}
        for row in rows
    ]


def test_binance_spot_volume_klines_table_name_contract(
    origo_assets: dict[str, object],
) -> None:
    assert origo_assets['VOLUME_KLINES_TABLE_NAME'] == 'binance_spot_volume_klines'


def test_binance_spot_volume_klines_schema_matches_spot_kline_statistics_contract(
    materialize_binance_spot_volume_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_volume_klines_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['VOLUME_KLINES_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == VOLUME_KLINE_COLUMNS
    assert [type_name for _, type_name, *_ in rows] == VOLUME_KLINE_COLUMN_TYPES
    assert _table_metadata(query_origo, str(origo_assets['VOLUME_KLINES_TABLE_NAME'])) == (
        'MergeTree',
        'toYYYYMM(start_datetime)',
        'start_datetime, end_datetime, volume_bar_id',
    )


def test_binance_spot_volume_klines_rows_match_fixture_with_exact_bar_range(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_volume_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setattr(
        origo_assets['refresh_binance_spot_volume_klines_origo_module'],
        'VOLUME_KLINE_SIZE',
        0.01,
    )

    result = materialize_binance_spot_volume_klines_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(VOLUME_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['VOLUME_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY volume_bar_id
        """
    )

    assert _rows_to_dicts(VOLUME_KLINE_COLUMNS, rows) == [
        {
            'start_datetime': '2024-01-01 00:00:00',
            'end_datetime': '2024-01-01 00:00:02',
            'volume_bar_id': 0,
            'open': 42000.1,
            'high': 42010.0,
            'low': 42000.1,
            'close': 42005.5,
            'mean': 42005.200000000004,
            'std': 4.047221268968605,
            'median': 42005.5,
            'iqr': 9.900000000001455,
            'volume': 0.0045000000000000005,
            'maker_ratio': 0.6666666666666666,
            'no_of_trades': 3,
            'open_liquidity': 42.000099999999996,
            'high_liquidity': 84.02,
            'low_liquidity': 42.000099999999996,
            'close_liquidity': 63.008250000000004,
            'liquidity_sum': 189.02835,
            'maker_volume': 0.0025,
            'maker_liquidity': 105.00835000000001,
        },
    ]


def test_same_partition_rerun_is_idempotent_for_volume_klines(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_volume_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setattr(
        origo_assets['refresh_binance_spot_volume_klines_origo_module'],
        'VOLUME_KLINE_SIZE',
        0.001,
    )

    first = materialize_binance_spot_volume_klines_assets(partition_key='2024-01-01')
    first_rows = query_origo(
        f"""
        SELECT
            {', '.join(VOLUME_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['VOLUME_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY volume_bar_id
        """
    )

    second = materialize_binance_spot_volume_klines_assets(partition_key='2024-01-01')
    second_rows = query_origo(
        f"""
        SELECT
            {', '.join(VOLUME_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['VOLUME_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY volume_bar_id
        """
    )

    assert first.success
    assert second.success
    assert first_rows == second_rows
    assert len(second_rows) == 3


def test_volume_kline_assets_job_and_schedule_are_registered(
    origo_definitions_module: object,
    origo_assets: dict[str, object],
) -> None:
    schedule_def = origo_definitions_module.daily_binance_spot_pipeline_schedule
    resolved_schedule_def = origo_definitions_module.defs.get_repository_def().get_schedule_def(
        'daily_binance_spot_pipeline_schedule'
    )
    data_source_job = origo_definitions_module.defs.get_job_def(
        'refresh_binance_spot_data_source_job'
    )
    create_table_job = origo_definitions_module.defs.get_job_def(
        'create_binance_spot_volume_klines_table_origo_job'
    )
    node_names = set(data_source_job.graph.node_dict.keys())
    volume_asset = origo_assets['refresh_binance_spot_volume_klines_origo']
    volume_deps = volume_asset.asset_deps[volume_asset.key]

    assert schedule_def.job.name == 'refresh_binance_spot_data_source_job'
    assert resolved_schedule_def.cron_schedule == '0 4 * * *'
    assert resolved_schedule_def.execution_timezone == 'UTC'
    assert resolved_schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert create_table_job.name == 'create_binance_spot_volume_klines_table_origo_job'
    assert node_names >= {
        'insert_daily_binance_spot_trades_to_origo',
        'refresh_binance_spot_klines_origo',
        'refresh_binance_spot_dollar_klines_origo',
        'refresh_binance_spot_volume_klines_origo',
        'refresh_aligned_1m_exchange_from_binance_spot_origo',
    }
    assert volume_deps == {
        origo_assets['create_binance_spot_volume_klines_table_origo'].key,
        origo_assets['insert_daily_binance_spot_trades_to_origo'].key,
    }
