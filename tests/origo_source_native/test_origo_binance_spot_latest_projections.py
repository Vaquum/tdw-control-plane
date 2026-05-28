from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta

import pytest
from dagster import DefaultScheduleStatus, build_schedule_context, materialize

from tdw_control_plane.utils import binance_spot_latest as latest_utils
from tdw_control_plane.utils.binance_spot_latest import (
    BinanceHistoricalTrade,
    LatestTradeBatch,
    LatestTradeIdBounds,
    fetch_closed_minute_trades,
    fetch_historical_trades_in_time_range,
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


def _has_two_day_delete_ttl(query: object) -> bool:
    text = str(query)
    return 'TTL' in text and 'DELETE' in text and (
        'INTERVAL 2 DAY' in text or 'toIntervalDay(2)' in text
    )


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


def _spot_trade_payload(row: tuple[object, ...]) -> dict[str, object]:
    trade_id, price, quantity, quote_quantity, _timestamp, maker, best_match, dt = row
    assert isinstance(trade_id, int)
    assert isinstance(price, float)
    assert isinstance(quantity, float)
    assert isinstance(quote_quantity, float)
    assert isinstance(maker, int)
    assert isinstance(best_match, int)
    assert isinstance(dt, datetime)
    return {
        'id': trade_id,
        'price': str(price),
        'qty': str(quantity),
        'quoteQty': str(quote_quantity),
        'time': int(dt.replace(tzinfo=UTC).timestamp() * 1000),
        'isBuyerMaker': bool(maker),
        'isBestMatch': bool(best_match),
    }


def _aggregate_trade_payload(row: tuple[object, ...]) -> dict[str, object]:
    trade_id, price, quantity, _quote_quantity, _timestamp, maker, best_match, dt = row
    assert isinstance(trade_id, int)
    assert isinstance(price, float)
    assert isinstance(quantity, float)
    assert isinstance(maker, int)
    assert isinstance(best_match, int)
    assert isinstance(dt, datetime)
    return {
        'a': trade_id,
        'p': str(price),
        'q': str(quantity),
        'f': trade_id,
        'l': trade_id,
        'T': int(dt.replace(tzinfo=UTC).timestamp() * 1000),
        'm': bool(maker),
        'M': bool(best_match),
    }


class _FakeBinanceResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        if isinstance(self._payload, dict) and self._payload.get('status') == 'error':
            raise RuntimeError('Fake Binance response failed.')

    def json(self) -> object:
        return self._payload


class _FixtureBinanceApi:
    def __init__(
        self,
        *,
        aggregate_rows: list[dict[str, object]],
        historical_rows: list[dict[str, object]],
    ) -> None:
        self._aggregate_rows = aggregate_rows
        self._historical_rows = historical_rows
        self.requests: list[tuple[str, dict[str, object]]] = []

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, object],
        headers: Mapping[str, str] | None = None,
        timeout: int,
    ) -> _FakeBinanceResponse:
        del headers, timeout
        endpoint = url.rsplit('/api/v3/', maxsplit=1)[1]
        request_params = dict(params)
        self.requests.append((endpoint, request_params))
        if endpoint == 'aggTrades':
            return _FakeBinanceResponse(self._aggregate_rows[:1])
        if endpoint != 'historicalTrades':
            raise AssertionError(f'Unexpected Binance endpoint {endpoint}')

        from_id = request_params['fromId']
        limit = request_params['limit']
        assert isinstance(from_id, int)
        assert isinstance(limit, int)
        rows = [row for row in self._historical_rows if row['id'] >= from_id]
        return _FakeBinanceResponse(rows[:limit])


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


def test_latest_trade_fetch_uses_aggtrade_start_and_forward_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_rows = load_expected_trade_rows(FIXTURE_DATE)
    api = _FixtureBinanceApi(
        aggregate_rows=[_aggregate_trade_payload(fixture_rows[0])],
        historical_rows=[_spot_trade_payload(row) for row in fixture_rows],
    )
    monkeypatch.setattr(latest_utils.requests, 'get', api.get)
    monkeypatch.setattr(latest_utils, 'BINANCE_HISTORICAL_TRADES_LIMIT', 2)

    batch = fetch_closed_minute_trades(
        'BTCUSDT',
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert [endpoint for endpoint, _params in api.requests] == [
        'aggTrades',
        'historicalTrades',
        'historicalTrades',
    ]
    assert api.requests[0][1] == {
        'symbol': 'BTCUSDT',
        'startTime': 1704067200000,
        'limit': 1,
    }
    assert [request[1]['fromId'] for request in api.requests[1:]] == [1001, 1003]
    assert [request[1]['limit'] for request in api.requests[1:]] == [2, 2]
    assert [row.trade_id for row in batch.rows] == [1001, 1002, 1003]
    assert batch.bounds.start_trade_id == 1001
    assert batch.bounds.end_trade_id == 1003


def test_latest_trade_range_is_half_open_when_next_trade_is_after_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    next_day_rows = load_expected_trade_rows('2024-01-02')
    api = _FixtureBinanceApi(
        aggregate_rows=[_aggregate_trade_payload(next_day_rows[0])],
        historical_rows=[_spot_trade_payload(row) for row in next_day_rows],
    )
    monkeypatch.setattr(latest_utils.requests, 'get', api.get)

    rows = fetch_historical_trades_in_time_range(
        'BTCUSDT',
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert rows == ()
    assert [endpoint for endpoint, _params in api.requests] == [
        'aggTrades',
        'historicalTrades',
    ]


def test_latest_trade_range_finishes_when_historical_endpoint_is_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_rows = load_expected_trade_rows(FIXTURE_DATE)[:2]
    api = _FixtureBinanceApi(
        aggregate_rows=[_aggregate_trade_payload(fixture_rows[0])],
        historical_rows=[_spot_trade_payload(row) for row in fixture_rows],
    )
    monkeypatch.setattr(latest_utils.requests, 'get', api.get)
    monkeypatch.setattr(latest_utils, 'BINANCE_HISTORICAL_TRADES_LIMIT', 2)

    rows = fetch_historical_trades_in_time_range(
        'BTCUSDT',
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
    )

    assert [row.trade_id for row in rows] == [1001, 1002]
    assert [endpoint for endpoint, _params in api.requests] == [
        'aggTrades',
        'historicalTrades',
        'historicalTrades',
    ]
    assert [request[1]['fromId'] for request in api.requests[1:]] == [1001, 1003]


def test_latest_closed_minute_still_rejects_non_contiguous_trade_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_rows = load_expected_trade_rows(FIXTURE_DATE)
    gapped_rows = [fixture_rows[0], fixture_rows[2]]
    api = _FixtureBinanceApi(
        aggregate_rows=[_aggregate_trade_payload(gapped_rows[0])],
        historical_rows=[_spot_trade_payload(row) for row in gapped_rows],
    )
    monkeypatch.setattr(latest_utils.requests, 'get', api.get)

    with pytest.raises(RuntimeError, match='not contiguous'):
        fetch_closed_minute_trades(
            'BTCUSDT',
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
        )


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
        FROM (
            SELECT
                kline_datetime,
                argMin(price, trade_id) AS open,
                max(price) AS high,
                min(price) AS low,
                argMax(price, trade_id) AS close,
                avg(price) AS mean,
                stddevPopStable(price) AS std,
                quantileExact(0.5)(price) AS median,
                quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
                sumKahan(quantity) AS volume,
                avg(is_buyer_maker) AS maker_ratio,
                count() AS no_of_trades,
                argMin(price * quantity, trade_id) AS open_liquidity,
                max(price * quantity) AS high_liquidity,
                min(price * quantity) AS low_liquidity,
                argMax(price * quantity, trade_id) AS close_liquidity,
                sum(price * quantity) AS liquidity_sum,
                sumKahan(is_buyer_maker * quantity) AS maker_volume,
                sum(is_buyer_maker * price * quantity) AS maker_liquidity
            FROM (
                SELECT
                    *,
                    toDateTime(60 * intDiv(toUnixTimestamp(datetime), 60)) AS kline_datetime
                FROM {ORIGO_DATABASE}.{origo_assets['RAW_TABLE_NAME']}
                WHERE toDate(datetime) = toDate('{FIXTURE_DATE}')
            )
            GROUP BY kline_datetime
        )
        ORDER BY kline_datetime
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
            toDateTime('{minute_text}') + (toDateTime(start_datetime) - toDateTime('2024-01-01 00:00:00')) AS start_datetime,
            toDateTime('{minute_text}') + (toDateTime(end_datetime) - toDateTime('2024-01-01 00:00:00')) AS end_datetime,
            {', '.join(DOLLAR_KLINE_COLUMNS[2:])}
        FROM (
            SELECT
                min(datetime) AS start_datetime,
                max(datetime) AS end_datetime,
                dollar_bar_id,
                argMin(price, trade_id) AS open,
                max(price) AS high,
                min(price) AS low,
                argMax(price, trade_id) AS close,
                avg(price) AS mean,
                stddevPopStable(price) AS std,
                quantileExact(0.5)(price) AS median,
                quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
                sumKahan(quantity) AS volume,
                avg(is_buyer_maker) AS maker_ratio,
                count() AS no_of_trades,
                argMin(price * quantity, trade_id) AS open_liquidity,
                max(price * quantity) AS high_liquidity,
                min(price * quantity) AS low_liquidity,
                argMax(price * quantity, trade_id) AS close_liquidity,
                sum(price * quantity) AS liquidity_sum,
                sumKahan(is_buyer_maker * quantity) AS maker_volume,
                sum(is_buyer_maker * price * quantity) AS maker_liquidity
            FROM (
                SELECT
                    *,
                    toUInt64(floor(running_quote_before / 1000000.0)) AS dollar_bar_id
                FROM (
                    SELECT
                        *,
                        greatest(
                            sum(quote_quantity) OVER (
                                ORDER BY datetime, trade_id
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            ) - quote_quantity,
                            0.0
                        ) AS running_quote_before
                    FROM {ORIGO_DATABASE}.{origo_assets['RAW_TABLE_NAME']}
                    WHERE toDate(datetime) = toDate('{FIXTURE_DATE}')
                )
            )
            GROUP BY dollar_bar_id
        )
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
    assert all(_has_two_day_delete_ttl(query) for query in create_queries)
    assert latest_job_names == {
        'refresh_binance_spot_latest_data_source_job',
        'create_binance_spot_latest_tables_origo_job',
    }
    assert not hasattr(origo_definitions_module, 'publish_binance_spot_latest_to_huggingface_job')
