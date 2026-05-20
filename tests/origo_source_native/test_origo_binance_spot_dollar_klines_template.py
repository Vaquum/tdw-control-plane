from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

import pytest
from dagster import DagsterInstance, DefaultScheduleStatus, materialize

from .helpers import ORIGO_DATABASE

DOLLAR_KLINE_COLUMNS = [
    'start_datetime',
    'end_datetime',
    'dollar_bar_id',
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
DOLLAR_KLINE_COLUMN_TYPES = [
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


def test_binance_spot_dollar_klines_table_name_contract(
    origo_assets: dict[str, object],
) -> None:
    assert origo_assets['DOLLAR_KLINES_TABLE_NAME'] == 'binance_spot_dollar_klines'


def test_binance_spot_dollar_klines_schema_matches_spot_kline_statistics_contract(
    materialize_binance_spot_dollar_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_dollar_klines_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DOLLAR_KLINES_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == DOLLAR_KLINE_COLUMNS
    assert [type_name for _, type_name, *_ in rows] == DOLLAR_KLINE_COLUMN_TYPES
    assert _table_metadata(query_origo, str(origo_assets['DOLLAR_KLINES_TABLE_NAME'])) == (
        'MergeTree',
        'toYYYYMM(start_datetime)',
        'start_datetime, end_datetime, dollar_bar_id',
    )


def test_binance_spot_dollar_klines_rows_match_fixture_with_exact_bar_range(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_dollar_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setattr(
        origo_assets['refresh_binance_spot_dollar_klines_origo_module'],
        'DOLLAR_KLINE_SIZE',
        200.0,
    )

    result = materialize_binance_spot_dollar_klines_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(DOLLAR_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY dollar_bar_id
        """
    )

    assert _rows_to_dicts(DOLLAR_KLINE_COLUMNS, rows) == [
        {
            'start_datetime': '2024-01-01 00:00:00',
            'end_datetime': '2024-01-01 00:00:02',
            'dollar_bar_id': 0,
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


def test_same_partition_rerun_is_idempotent_for_dollar_klines(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_dollar_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setattr(
        origo_assets['refresh_binance_spot_dollar_klines_origo_module'],
        'DOLLAR_KLINE_SIZE',
        40.0,
    )

    first = materialize_binance_spot_dollar_klines_assets(partition_key='2024-01-01')
    first_rows = query_origo(
        f"""
        SELECT
            {', '.join(DOLLAR_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY dollar_bar_id
        """
    )

    second = materialize_binance_spot_dollar_klines_assets(partition_key='2024-01-01')
    second_rows = query_origo(
        f"""
        SELECT
            {', '.join(DOLLAR_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        ORDER BY dollar_bar_id
        """
    )

    assert first.success
    assert second.success
    assert first_rows == second_rows
    assert len(second_rows) == 3


def test_dollar_kline_refresh_fails_before_replacing_when_raw_partition_is_absent(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    instance = DagsterInstance.ephemeral()
    first = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_daily_spot_trades_table_origo'],
            origo_assets['create_binance_spot_dollar_klines_table_origo'],
            origo_assets['insert_daily_binance_spot_trades_to_origo'],
            origo_assets['refresh_binance_spot_dollar_klines_origo'],
        ],
        instance=instance,
        partition_key='2024-01-01',
    )
    before_rows = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        """
    )
    query_origo(
        f"""
        ALTER TABLE {ORIGO_DATABASE}.{origo_assets['RAW_TABLE_NAME']}
        DELETE WHERE toDate(datetime) = toDate('2024-01-01')
        SETTINGS mutations_sync = 2
        """
    )

    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_daily_spot_trades_table_origo'],
            origo_assets['create_binance_spot_dollar_klines_table_origo'],
            origo_assets['refresh_binance_spot_dollar_klines_origo'],
        ],
        instance=instance,
        partition_key='2024-01-01',
        raise_on_error=False,
    )
    after_rows = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['DOLLAR_KLINES_TABLE_NAME']}
        WHERE toDate(start_datetime) = toDate('2024-01-01')
        """
    )

    assert first.success
    assert before_rows[0][0] > 0
    assert not result.success
    assert before_rows == after_rows


def test_dollar_kline_assets_job_and_schedule_are_registered(
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
        'create_binance_spot_dollar_klines_table_origo_job'
    )
    node_names = set(data_source_job.graph.node_dict.keys())
    dollar_asset = origo_assets['refresh_binance_spot_dollar_klines_origo']
    dollar_deps = dollar_asset.asset_deps[dollar_asset.key]

    assert schedule_def.job.name == 'refresh_binance_spot_data_source_job'
    assert resolved_schedule_def.cron_schedule == '0 4 * * *'
    assert resolved_schedule_def.execution_timezone == 'UTC'
    assert resolved_schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert create_table_job.name == 'create_binance_spot_dollar_klines_table_origo_job'
    assert node_names >= {
        'insert_daily_binance_spot_trades_to_origo',
        'refresh_binance_spot_klines_origo',
        'refresh_binance_spot_dollar_klines_origo',
        'refresh_aligned_1m_exchange_from_binance_spot_origo',
    }
    assert dollar_deps == {
        origo_assets['create_binance_spot_dollar_klines_table_origo'].key,
        origo_assets['insert_daily_binance_spot_trades_to_origo'].key,
    }


def test_dollar_kline_backfill_job_is_downstream_only(
    origo_definitions_module: object,
) -> None:
    backfill_job = origo_definitions_module.defs.get_job_def(
        'backfill_binance_spot_dollar_klines_origo_job'
    )
    node_names = set(backfill_job.graph.node_dict.keys())

    assert node_names == {'refresh_binance_spot_dollar_klines_origo'}
    assert backfill_job.partitions_def is not None
    assert '2024-01-01' in backfill_job.partitions_def.get_partition_keys(
        current_time=datetime(2024, 1, 3)
    )
