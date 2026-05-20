from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import pytest
from dagster import DefaultScheduleStatus

from .helpers import ORIGO_DATABASE

DOLLAR_IMBALANCE_KLINE_COLUMNS = [
    'start_datetime',
    'end_datetime',
    'dollar_imbalance_bar_id',
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
    'taker_buy_liquidity',
    'taker_sell_liquidity',
    'dollar_imbalance',
]
DOLLAR_IMBALANCE_KLINE_COLUMN_TYPES = [
    'DateTime',
    'DateTime',
    'UInt64',
    *(['Float64'] * 10),
    'UInt64',
    *(['Float64'] * 10),
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


def test_binance_spot_dollar_imbalance_klines_table_name_contract(
    origo_assets: dict[str, object],
) -> None:
    assert (
        origo_assets['DOLLAR_IMBALANCE_KLINES_TABLE_NAME']
        == 'binance_spot_dollar_imbalance_klines'
    )


def test_binance_spot_dollar_imbalance_klines_schema_matches_contract(
    materialize_binance_spot_dollar_imbalance_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_dollar_imbalance_klines_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DOLLAR_IMBALANCE_KLINES_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == DOLLAR_IMBALANCE_KLINE_COLUMNS
    assert [type_name for _, type_name, *_ in rows] == DOLLAR_IMBALANCE_KLINE_COLUMN_TYPES
    assert _table_metadata(
        query_origo, str(origo_assets['DOLLAR_IMBALANCE_KLINES_TABLE_NAME'])
    ) == (
        'MergeTree',
        'toYYYYMM(start_datetime)',
        'start_datetime, end_datetime, dollar_imbalance_bar_id',
    )


def test_binance_spot_dollar_imbalance_klines_rows_match_fixture_with_exact_bar_range(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_dollar_imbalance_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setattr(
        origo_assets['refresh_binance_spot_dollar_imbalance_klines_origo_module'],
        'DOLLAR_IMBALANCE_KLINE_SIZE',
        100.0,
    )

    result = materialize_binance_spot_dollar_imbalance_klines_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(DOLLAR_IMBALANCE_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_IMBALANCE_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY dollar_imbalance_bar_id
        """
    )

    assert _rows_to_dicts(DOLLAR_IMBALANCE_KLINE_COLUMNS, rows) == [
        {
            'start_datetime': '2024-01-01 00:00:00',
            'end_datetime': '2024-01-01 00:00:02',
            'dollar_imbalance_bar_id': 0,
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
            'taker_buy_liquidity': 84.02,
            'taker_sell_liquidity': 105.00835000000001,
            'dollar_imbalance': -20.98835000000001,
        },
    ]


def test_same_partition_rerun_is_idempotent_for_dollar_imbalance_klines(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_dollar_imbalance_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setattr(
        origo_assets['refresh_binance_spot_dollar_imbalance_klines_origo_module'],
        'DOLLAR_IMBALANCE_KLINE_SIZE',
        40.0,
    )

    first = materialize_binance_spot_dollar_imbalance_klines_assets(partition_key='2024-01-01')
    first_rows = query_origo(
        f"""
        SELECT
            {', '.join(DOLLAR_IMBALANCE_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_IMBALANCE_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY dollar_imbalance_bar_id
        """
    )

    second = materialize_binance_spot_dollar_imbalance_klines_assets(partition_key='2024-01-01')
    second_rows = query_origo(
        f"""
        SELECT
            {', '.join(DOLLAR_IMBALANCE_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_IMBALANCE_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY dollar_imbalance_bar_id
        """
    )

    assert first.success
    assert second.success
    assert first_rows == second_rows
    assert len(second_rows) == 3


def test_dollar_imbalance_kline_assets_job_and_schedule_are_registered(
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
        'create_binance_spot_dollar_imbalance_klines_table_origo_job'
    )
    node_names = set(data_source_job.graph.node_dict.keys())
    dollar_imbalance_asset = origo_assets['refresh_binance_spot_dollar_imbalance_klines_origo']
    dollar_imbalance_deps = dollar_imbalance_asset.asset_deps[dollar_imbalance_asset.key]

    assert schedule_def.job.name == 'refresh_binance_spot_data_source_job'
    assert resolved_schedule_def.cron_schedule == '0 4 * * *'
    assert resolved_schedule_def.execution_timezone == 'UTC'
    assert resolved_schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert create_table_job.name == 'create_binance_spot_dollar_imbalance_klines_table_origo_job'
    assert node_names >= {
        'insert_daily_binance_spot_trades_to_origo',
        'refresh_binance_spot_klines_origo',
        'refresh_binance_spot_dollar_imbalance_klines_origo',
        'refresh_aligned_1m_exchange_from_binance_spot_origo',
    }
    assert dollar_imbalance_deps == {
        origo_assets['create_binance_spot_dollar_imbalance_klines_table_origo'].key,
        origo_assets['insert_daily_binance_spot_trades_to_origo'].key,
    }
