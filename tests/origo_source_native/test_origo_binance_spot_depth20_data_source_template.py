from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from dagster import DefaultScheduleStatus, build_schedule_context, materialize

from .helpers import ORIGO_DATABASE

DEPTH20_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / 'fixtures'
    / 'binance'
    / 'spot'
    / 'depth20'
    / 'BTCUSDT-depth20-2026-05-13T13-23.ndjson'
)
DEPTH20_FIXTURE_CHECKSUM_PATH = DEPTH20_FIXTURE_PATH.with_suffix(
    f'{DEPTH20_FIXTURE_PATH.suffix}.CHECKSUM'
)
DEPTH20_MINUTE_SQL = '2026-05-13 13:23:00'
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


def _fixture_lines():
    return [json.loads(line) for line in DEPTH20_FIXTURE_PATH.read_text().splitlines()]


def _expected_book_scalars() -> tuple[float, float, float, float, float]:
    last_depth = _fixture_lines()[-1]['d']
    bids = [(float(price), float(quantity)) for price, quantity in last_depth['bids']]
    asks = [(float(price), float(quantity)) for price, quantity in last_depth['asks']]
    book_mid_price = (bids[0][0] + asks[0][0]) / 2
    book_spread_bps = ((asks[0][0] - bids[0][0]) / book_mid_price) * 10000
    book_bid_depth_20_notional = sum(price * quantity for price, quantity in bids)
    book_ask_depth_20_notional = sum(price * quantity for price, quantity in asks)
    book_imbalance_20 = (
        (book_bid_depth_20_notional - book_ask_depth_20_notional)
        / (book_bid_depth_20_notional + book_ask_depth_20_notional)
    )
    return (
        book_mid_price,
        book_spread_bps,
        book_bid_depth_20_notional,
        book_ask_depth_20_notional,
        book_imbalance_20,
    )


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


def test_binance_spot_depth20_fixture_is_real_history_payload() -> None:
    checksum = hashlib.sha256(DEPTH20_FIXTURE_PATH.read_bytes()).hexdigest()
    expected_checksum = DEPTH20_FIXTURE_CHECKSUM_PATH.read_text().split()[0]
    rows = _fixture_lines()

    assert checksum == expected_checksum
    assert [row['t'] for row in rows] == [1778678580141, 1778678639138]
    assert [row['d']['lastUpdateId'] for row in rows] == [93613301126, 93613326256]
    assert all(len(row['d']['bids']) == 20 for row in rows)
    assert all(len(row['d']['asks']) == 20 for row in rows)


def test_sync_binance_spot_depth20_snapshots_reads_history_api_fixture_shape(
    materialize_binance_spot_depth20_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_depth20_data_source_assets()
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            source_timestamp_ms,
            last_update_id,
            length(bids),
            length(asks),
            bids[1].1,
            bids[1].2,
            asks[1].1,
            asks[1].2
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']} FINAL
        ORDER BY datetime
        """
    )

    assert rows == [
        (1778678580141, 93613301126, 20, 20, 80262.78, 2.26397, 80262.79, 3.90319),
        (1778678639138, 93613326256, 20, 20, 80249.27, 0.21104, 80249.28, 4.83203),
    ]


def test_sync_binance_spot_depth20_snapshots_is_idempotent_for_minute(
    materialize_binance_spot_depth20_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    first = materialize_binance_spot_depth20_data_source_assets()
    second = materialize_binance_spot_depth20_data_source_assets()

    assert first.success
    assert second.success

    rows = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH20_SNAPSHOTS_TABLE_NAME']} FINAL
        WHERE datetime >= toDateTime64('{DEPTH20_MINUTE_SQL}.000', 3)
          AND datetime < toDateTime64('2026-05-13 13:24:00.000', 3)
        """
    )
    assert rows == [(2,)]


def test_refresh_binance_spot_depth20_1m_uses_last_row_of_last_completed_minute(
    materialize_binance_spot_depth20_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_depth20_data_source_assets()
    assert result.success

    rows = query_origo(
        f"""
        SELECT datetime, book_mid_price
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH20_1M_TABLE_NAME']} FINAL
        ORDER BY datetime
        """
    )

    assert rows == [(datetime(2026, 5, 13, 13, 23), 80249.275)]


def test_refresh_binance_spot_depth20_1m_computes_book_scalar_columns(
    materialize_binance_spot_depth20_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_depth20_data_source_assets()
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            book_mid_price,
            book_spread_bps,
            book_bid_depth_20_notional,
            book_ask_depth_20_notional,
            book_imbalance_20
        FROM {ORIGO_DATABASE}.{origo_assets['DEPTH20_1M_TABLE_NAME']} FINAL
        WHERE datetime = toDateTime('{DEPTH20_MINUTE_SQL}')
        """
    )

    assert len(rows) == 1
    assert rows[0] == pytest.approx(_expected_book_scalars())


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
