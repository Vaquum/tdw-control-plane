from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest
from dagster import DefaultScheduleStatus, build_schedule_context, materialize

from tdw_control_plane.utils.binance_spot_latest import (
    BinanceHistoricalTrade,
    LatestTradeBatch,
    LatestTradeIdBounds,
)

from .helpers import ORIGO_DATABASE, load_expected_trade_rows

KLINE_COLUMNS = [
    'datetime',
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
DOLLAR_KLINE_COLUMNS = [
    'start_datetime',
    'end_datetime',
    'dollar_bar_id',
    *KLINE_COLUMNS[1:],
]
TIME_CUT_TABLES = [
    'binance_spot_15m_klines_latest',
    'binance_spot_30m_klines_latest',
    'binance_spot_1h_klines_latest',
    'binance_spot_2h_klines_latest',
    'binance_spot_4h_klines_latest',
]
DOLLAR_CUT_TABLES = [
    'binance_spot_15M_dollar_klines_latest',
    'binance_spot_30M_dollar_klines_latest',
    'binance_spot_60M_dollar_klines_latest',
    'binance_spot_120M_dollar_klines_latest',
    'binance_spot_240M_dollar_klines_latest',
]
LATEST_MINUTE_TAG = 'binance_spot_latest_minute_start'
FIXTURE_DATE = '2024-01-01'


def _minute_key(value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')


def _unexpired_minute(offset_minutes: int = 0) -> datetime:
    base = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(hours=1)
    return base + timedelta(minutes=offset_minutes)


def _fixture_latest_batch(minute_start: datetime) -> LatestTradeBatch:
    source_minute = datetime.fromisoformat(FIXTURE_DATE)
    fixture_rows = load_expected_trade_rows(FIXTURE_DATE)
    trades: list[BinanceHistoricalTrade] = []
    for row in fixture_rows:
        trade_id, price, quantity, quote_quantity, timestamp, maker, best_match, dt = row
        assert isinstance(trade_id, int)
        assert isinstance(price, float)
        assert isinstance(quantity, float)
        assert isinstance(quote_quantity, float)
        assert isinstance(timestamp, int)
        assert isinstance(maker, int)
        assert isinstance(best_match, int)
        assert isinstance(dt, datetime)
        shifted_datetime = minute_start.replace(tzinfo=None) + (dt - source_minute)
        trades.append(
            BinanceHistoricalTrade(
                trade_id=trade_id,
                price=price,
                quantity=quantity,
                quote_quantity=quote_quantity,
                timestamp=int(shifted_datetime.replace(tzinfo=UTC).timestamp() * 1000),
                is_buyer_maker=bool(maker),
                is_best_match=bool(best_match),
                datetime=shifted_datetime,
            )
        )

    minute_start = minute_start.astimezone(UTC)
    return LatestTradeBatch(
        bounds=LatestTradeIdBounds(
            minute_start=minute_start.replace(tzinfo=None),
            minute_end=(minute_start + timedelta(minutes=1)).replace(tzinfo=None),
            start_trade_id=trades[0].trade_id,
            end_trade_id=trades[-1].trade_id,
        ),
        rows=tuple(trades),
    )


def _install_fixture_fetch(
    monkeypatch: pytest.MonkeyPatch,
    origo_assets: dict[str, object],
) -> None:
    def _fetch(symbol: str, minute_start: datetime, minute_end: datetime) -> LatestTradeBatch:
        assert symbol == 'BTCUSDT'
        assert (minute_end - minute_start).total_seconds() == 60
        return _fixture_latest_batch(minute_start)

    monkeypatch.setattr(
        origo_assets['sync_binance_spot_trades_latest_origo_module'],
        'fetch_closed_minute_trades',
        _fetch,
    )


def _materialize_latest_sync(origo_assets: dict[str, object], *, minute_start: str) -> object:
    return materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_latest_tables_origo'],
            origo_assets['sync_binance_spot_trades_latest_origo'],
        ],
        tags={LATEST_MINUTE_TAG: minute_start},
    )


def _evaluate_latest_schedule(origo_definitions_module: object, scheduled_time: datetime) -> object:
    repository_def = origo_definitions_module.defs.get_repository_def()
    context = build_schedule_context(
        scheduled_execution_time=scheduled_time,
        repository_def=repository_def,
    )
    schedule_def = repository_def.get_schedule_def('binance_spot_latest_1m_schedule')
    return schedule_def.evaluate_tick(context).run_requests


def test_latest_trade_ingest_requires_exact_closed_minute_id_range(
    monkeypatch: pytest.MonkeyPatch,
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    _install_fixture_fetch(monkeypatch, origo_assets)
    minute_start = _unexpired_minute()
    minute_text = minute_start.strftime('%Y-%m-%d %H:%M:%S')

    result = _materialize_latest_sync(origo_assets, minute_start=_minute_key(minute_start))
    ledger_rows = query_origo(
        f"""
        SELECT start_trade_id, end_trade_id, row_count, status
        FROM {ORIGO_DATABASE}.binance_spot_trades_latest_ingestion
        WHERE minute_start = toDateTime('{minute_text}')
        """
    )
    raw_rows = query_origo(
        f"""
        SELECT min(trade_id), max(trade_id), count()
        FROM {ORIGO_DATABASE}.binance_spot_trades_latest
        WHERE minute_start = toDateTime('{minute_text}')
        """
    )

    assert result.success
    assert ledger_rows == [(1001, 1003, 3, 'success')]
    assert raw_rows == [(1001, 1003, 3)]


def test_latest_schedule_requests_last_closed_minute_once_per_minute(
    origo_definitions_module: object,
) -> None:
    schedule_def = origo_definitions_module.binance_spot_latest_1m_schedule
    resolved_schedule_def = origo_definitions_module.defs.get_repository_def().get_schedule_def(
        'binance_spot_latest_1m_schedule'
    )
    data_source_job = origo_definitions_module.defs.get_job_def(
        'refresh_binance_spot_latest_data_source_job'
    )
    node_names = set(data_source_job.graph.node_dict.keys())
    run_requests = _evaluate_latest_schedule(
        origo_definitions_module,
        datetime(2024, 1, 1, 0, 2, 31, tzinfo=UTC),
    )

    assert schedule_def.job.name == 'refresh_binance_spot_latest_data_source_job'
    assert resolved_schedule_def.cron_schedule == '* * * * *'
    assert resolved_schedule_def.execution_timezone == 'UTC'
    assert resolved_schedule_def.default_status == DefaultScheduleStatus.RUNNING
    assert node_names == {
        'sync_binance_spot_trades_latest_origo',
        'refresh_binance_spot_klines_latest_origo',
        'refresh_binance_spot_dollar_klines_latest_origo',
        'refresh_binance_spot_latest_cuts_origo',
        'cleanup_binance_spot_latest_origo',
    }
    assert [request.run_key for request in run_requests] == [
        'binance_spot_latest::2024-01-01T00:01:00Z'
    ]
    assert [request.tags[LATEST_MINUTE_TAG] for request in run_requests] == ['2024-01-01T00:01:00Z']


def test_latest_watermark_advances_only_through_contiguous_successful_minutes(
    monkeypatch: pytest.MonkeyPatch,
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    _install_fixture_fetch(monkeypatch, origo_assets)
    first_minute = _unexpired_minute()
    gap_minute = _unexpired_minute(2)

    first = _materialize_latest_sync(origo_assets, minute_start=_minute_key(first_minute))
    second = _materialize_latest_sync(origo_assets, minute_start=_minute_key(gap_minute))
    rows = query_origo(
        f"""
        SELECT watermark_minute
        FROM {ORIGO_DATABASE}.binance_spot_latest_watermarks
        WHERE layer = 'trades'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )

    assert first.success
    assert second.success
    assert rows == [(first_minute.replace(tzinfo=None),)]


def test_latest_foundation_tables_match_authoritative_time_and_dollar_sql(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_data_source_assets: Callable[..., object],
    materialize_binance_spot_dollar_klines_assets: Callable[..., object],
    materialize_binance_spot_latest_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    _install_fixture_fetch(monkeypatch, origo_assets)
    minute_start = _unexpired_minute()
    minute_text = minute_start.strftime('%Y-%m-%d %H:%M:%S')

    daily_time = materialize_binance_spot_data_source_assets(partition_key=FIXTURE_DATE)
    daily_dollar = materialize_binance_spot_dollar_klines_assets(partition_key=FIXTURE_DATE)
    latest = materialize_binance_spot_latest_assets(minute_start=_minute_key(minute_start))
    daily_kline_rows = query_origo(
        f"""
        SELECT toDateTime('{minute_text}') AS datetime, {', '.join(KLINE_COLUMNS[1:])}
        FROM {ORIGO_DATABASE}.binance_spot_klines
        WHERE datetime = toDateTime('2024-01-01 00:00:00')
        """
    )
    latest_kline_rows = query_origo(
        f"""
        SELECT {', '.join(KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.binance_spot_klines_latest
        WHERE datetime = toDateTime('{minute_text}')
        """
    )
    daily_dollar_rows = query_origo(
        f"""
        SELECT
            toDateTime('{minute_text}') + (start_datetime - toDateTime('2024-01-01 00:00:00')) AS start_datetime,
            toDateTime('{minute_text}') + (end_datetime - toDateTime('2024-01-01 00:00:00')) AS end_datetime,
            {', '.join(DOLLAR_KLINE_COLUMNS[2:])}
        FROM {ORIGO_DATABASE}.binance_spot_dollar_klines
        ORDER BY dollar_bar_id
        """
    )
    latest_dollar_rows = query_origo(
        f"""
        SELECT {', '.join(DOLLAR_KLINE_COLUMNS)}
        FROM {ORIGO_DATABASE}.binance_spot_dollar_klines_latest
        WHERE start_datetime >= toDateTime('{minute_text}')
          AND start_datetime < toDateTime('{minute_text}') + INTERVAL 1 MINUTE
        ORDER BY dollar_bar_id
        """
    )

    assert daily_time.success
    assert daily_dollar.success
    assert latest.success
    assert latest_kline_rows == daily_kline_rows
    assert latest_dollar_rows == daily_dollar_rows


def test_latest_child_cuts_cover_time_and_dollar_hf_cadences_from_foundations(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_latest_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    _install_fixture_fetch(monkeypatch, origo_assets)
    minute_start = _unexpired_minute()

    result = materialize_binance_spot_latest_assets(minute_start=_minute_key(minute_start))
    time_cut_rows = query_origo(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name IN ({', '.join(repr(table_name) for table_name in TIME_CUT_TABLES)})
        ORDER BY name
        """
    )
    dollar_cut_rows = query_origo(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name IN ({', '.join(repr(table_name) for table_name in DOLLAR_CUT_TABLES)})
        ORDER BY name
        """
    )
    counts = [
        query_origo(f'SELECT count() FROM {ORIGO_DATABASE}.{table_name}')[0][0]
        for table_name in [*TIME_CUT_TABLES, *DOLLAR_CUT_TABLES]
    ]

    assert result.success
    assert [row[0] for row in time_cut_rows] == sorted(TIME_CUT_TABLES)
    assert [row[0] for row in dollar_cut_rows] == sorted(DOLLAR_CUT_TABLES)
    assert counts == [1] * 10


def test_latest_retention_and_hf_boundary_are_enforced(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
    origo_definitions_module: object,
) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_spot_latest_tables_origo'],
        ]
    )
    latest_tables = [
        'binance_spot_trades_latest',
        'binance_spot_trades_latest_ingestion',
        'binance_spot_klines_latest',
        'binance_spot_dollar_klines_latest',
        *TIME_CUT_TABLES,
        *DOLLAR_CUT_TABLES,
    ]
    create_queries = [
        query_origo(f'SHOW CREATE TABLE {ORIGO_DATABASE}.{table_name}')[0][0]
        for table_name in latest_tables
    ]
    latest_job_names = {
        name
        for name in [
            'refresh_binance_spot_latest_data_source_job',
            'create_binance_spot_latest_tables_origo_job',
        ]
        if origo_definitions_module.defs.get_job_def(name).name == name
    }

    assert result.success
    assert all(
        'TTL' in str(query) and 'INTERVAL 2 DAY DELETE' in str(query) for query in create_queries
    )
    assert latest_job_names == {
        'refresh_binance_spot_latest_data_source_job',
        'create_binance_spot_latest_tables_origo_job',
    }
    assert not hasattr(origo_definitions_module, 'publish_binance_spot_latest_to_huggingface_job')
