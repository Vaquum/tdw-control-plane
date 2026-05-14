from __future__ import annotations

from datetime import datetime, timezone

from dagster import DefaultScheduleStatus, build_schedule_context, materialize

from .helpers import ORIGO_DATABASE

DEPTH20_MINUTE_CONFIG = '2026-05-13T13:23:00+00:00'
DEPTH20_EXPECTED_COLUMNS = [
    'datetime',
    'book_mid_price',
    'book_spread_bps',
    'book_bid_depth_20_notional',
    'book_ask_depth_20_notional',
    'book_imbalance_20',
]


def _table_metadata(query_origo, table_name: str) -> tuple[str, str, str]:
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


def _depth20_run_config() -> dict[str, object]:
    return {
        'ops': {
            'sync_binance_spot_depth20_snapshots_to_origo': {
                'config': {'minute_start': DEPTH20_MINUTE_CONFIG}
            },
            'refresh_binance_spot_depth20_1m_origo': {
                'config': {'minute_start': DEPTH20_MINUTE_CONFIG}
            },
        }
    }


def test_binance_spot_depth20_snapshots_table_name_contract(
    origo_assets: dict[str, object],
) -> None:
    assert origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME'] == 'binance_spot_depth20_snapshots'


def test_binance_spot_depth20_1m_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['DEPTH20_1M_TABLE_NAME'] == 'binance_spot_depth20_1m'


def test_binance_spot_depth20_source_native_schema_matches_history_payload(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth20_snapshots_table_origo'],
        ]
    )
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == [
        'datetime',
        'source_timestamp_ms',
        'last_update_id',
        'bids',
        'asks',
    ]
    assert [type_name for _, type_name, *_ in rows] == [
        'DateTime64(3)',
        'UInt64',
        'UInt64',
        'Array(Tuple(Float64, Float64))',
        'Array(Tuple(Float64, Float64))',
    ]
    assert _table_metadata(query_origo, origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']) == (
        'ReplacingMergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_depth20_1m_schema_contains_datetime_and_five_scalar_book_columns(
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_depth20_1m_table_origo'],
        ]
    )
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['DEPTH20_1M_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == DEPTH20_EXPECTED_COLUMNS
    assert [type_name for _, type_name, *_ in rows] == ['DateTime', *(['Float64'] * 5)]
    assert _table_metadata(query_origo, origo_assets['DEPTH20_1M_TABLE_NAME']) == (
        'ReplacingMergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_depth20_data_source_job_and_schedule_are_registered(
    origo_definitions_module,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    schedule_def = repository_def.get_schedule_def('binance_spot_depth20_1m_schedule')
    job_def = origo_definitions_module.defs.get_job_def('refresh_binance_spot_depth20_data_source_job')
    context = build_schedule_context(
        scheduled_execution_time=datetime(2026, 5, 13, 13, 24, tzinfo=timezone.utc),
        repository_def=repository_def,
    )
    tick = schedule_def.evaluate_tick(context)

    assert schedule_def.cron_schedule == '* * * * *'
    assert schedule_def.execution_timezone == 'UTC'
    assert schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert set(job_def.graph.node_dict.keys()) >= {
        'sync_binance_spot_depth20_snapshots_to_origo',
        'refresh_binance_spot_depth20_1m_origo',
    }
    assert len(tick.run_requests) == 1
    assert tick.run_requests[0].run_key == 'binance_spot_depth20::2026-05-13T13:23:00+00:00'
    assert tick.run_requests[0].run_config == _depth20_run_config()
