import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import requests

DEFAULT_BINANCE_SPOT_REST_BASE_URL = 'https://api.binance.com'
BINANCE_HISTORICAL_TRADES_LIMIT = 1000


@dataclass(frozen=True)
class LatestTradeIdBounds:
    minute_start: datetime
    minute_end: datetime
    start_trade_id: int
    end_trade_id: int


@dataclass(frozen=True)
class BinanceHistoricalTrade:
    trade_id: int
    price: float
    quantity: float
    quote_quantity: float
    timestamp: int
    is_buyer_maker: bool
    is_best_match: bool
    datetime: datetime


@dataclass(frozen=True)
class LatestTradeBatch:
    bounds: LatestTradeIdBounds
    rows: tuple[BinanceHistoricalTrade, ...]


def _binance_rest_base_url() -> str:
    return os.environ.get('BINANCE_SPOT_REST_BASE_URL', DEFAULT_BINANCE_SPOT_REST_BASE_URL)


def _binance_api_headers() -> dict[str, str]:
    api_key = os.environ.get('BINANCE_API_KEY')
    if api_key:
        return {'X-MBX-APIKEY': api_key}
    return {}


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _millis(value: datetime) -> int:
    if value.tzinfo is None:
        aware = value.replace(tzinfo=UTC)
    else:
        aware = value.astimezone(UTC)
    return int(aware.timestamp() * 1000)


def _response_payload(response: requests.Response) -> object:
    response.raise_for_status()
    payload: object = response.json()
    return payload


def _trade_items(payload: object) -> list[Mapping[str, object]]:
    if not isinstance(payload, list):
        raise RuntimeError('Binance trade response must be a list.')

    items = cast(list[object], payload)
    rows: list[Mapping[str, object]] = []
    for item in items:
        if not isinstance(item, Mapping):
            raise RuntimeError('Binance trade row must be an object.')
        row: dict[str, object] = {}
        for key, value in cast(Mapping[object, object], item).items():
            if not isinstance(key, str):
                raise RuntimeError('Binance trade row keys must be strings.')
            row[key] = value
        rows.append(row)
    return rows


def _required_int(row: Mapping[str, object], key: str) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'Binance trade field {key} must be an integer.')
    return value


def _required_float_text(row: Mapping[str, object], key: str) -> float:
    value = row.get(key)
    if not isinstance(value, str):
        raise RuntimeError(f'Binance trade field {key} must be a string.')
    return float(value)


def _required_bool(row: Mapping[str, object], key: str) -> bool:
    value = row.get(key)
    if not isinstance(value, bool):
        raise RuntimeError(f'Binance trade field {key} must be a boolean.')
    return value


def _parse_historical_trade(row: Mapping[str, object]) -> BinanceHistoricalTrade:
    timestamp = _required_int(row, 'time')
    return BinanceHistoricalTrade(
        trade_id=_required_int(row, 'id'),
        price=_required_float_text(row, 'price'),
        quantity=_required_float_text(row, 'qty'),
        quote_quantity=_required_float_text(row, 'quoteQty'),
        timestamp=timestamp,
        is_buyer_maker=_required_bool(row, 'isBuyerMaker'),
        is_best_match=_required_bool(row, 'isBestMatch'),
        datetime=datetime.fromtimestamp(timestamp / 1000.0, tz=UTC).replace(tzinfo=None),
    )


def _historical_trades(
    symbol: str, *, from_id: int, limit: int
) -> tuple[BinanceHistoricalTrade, ...]:
    response = requests.get(
        f'{_binance_rest_base_url()}/api/v3/historicalTrades',
        params={'symbol': symbol, 'fromId': from_id, 'limit': limit},
        headers=_binance_api_headers(),
        timeout=30,
    )
    return tuple(
        _parse_historical_trade(item) for item in _trade_items(_response_payload(response))
    )


def _recent_trade_id(symbol: str) -> int:
    response = requests.get(
        f'{_binance_rest_base_url()}/api/v3/trades',
        params={'symbol': symbol, 'limit': 1},
        timeout=30,
    )
    rows = tuple(
        _parse_historical_trade(item) for item in _trade_items(_response_payload(response))
    )
    if len(rows) != 1:
        raise RuntimeError('Binance recent-trades endpoint must return one row.')
    return rows[0].trade_id


def _first_trade_id_at_or_after(symbol: str, target_timestamp_ms: int) -> int:
    low = 0
    high = _recent_trade_id(symbol)
    candidate = high + 1

    while low <= high:
        midpoint = (low + high) // 2
        rows = _historical_trades(symbol, from_id=midpoint, limit=1)
        if not rows:
            raise RuntimeError(f'No Binance trade returned at or after id {midpoint}.')

        trade = rows[0]
        if trade.timestamp >= target_timestamp_ms:
            candidate = trade.trade_id
            high = midpoint - 1
        else:
            low = trade.trade_id + 1

    return candidate


def _fetch_trade_range(
    symbol: str,
    *,
    start_trade_id: int,
    end_trade_id: int,
) -> tuple[BinanceHistoricalTrade, ...]:
    rows: list[BinanceHistoricalTrade] = []
    next_trade_id = start_trade_id
    while next_trade_id <= end_trade_id:
        limit = min(BINANCE_HISTORICAL_TRADES_LIMIT, end_trade_id - next_trade_id + 1)
        page = _historical_trades(symbol, from_id=next_trade_id, limit=limit)
        if not page:
            raise RuntimeError(f'No Binance trades returned from id {next_trade_id}.')
        rows.extend(page)
        next_trade_id = page[-1].trade_id + 1

    return tuple(row for row in rows if start_trade_id <= row.trade_id <= end_trade_id)


def _validate_closed_minute_batch(
    rows: tuple[BinanceHistoricalTrade, ...],
    *,
    start_trade_id: int,
    end_trade_id: int,
    minute_start_ms: int,
    minute_end_ms: int,
) -> None:
    if not rows:
        raise RuntimeError('Closed-minute Binance trade batch was empty.')

    expected_ids = tuple(range(start_trade_id, end_trade_id + 1))
    actual_ids = tuple(row.trade_id for row in rows)
    if actual_ids != expected_ids:
        raise RuntimeError(
            f'Closed-minute Binance trade ids are not contiguous: '
            f'expected {expected_ids[0]}..{expected_ids[-1]}, '
            f'got {actual_ids[0]}..{actual_ids[-1]}.'
        )

    for row in rows:
        if row.timestamp < minute_start_ms or row.timestamp >= minute_end_ms:
            raise RuntimeError(
                f'Binance trade {row.trade_id} is outside the requested closed minute.'
            )


def fetch_closed_minute_trades(
    symbol: str,
    minute_start: datetime,
    minute_end: datetime,
) -> LatestTradeBatch:
    minute_start_ms = _millis(minute_start)
    minute_end_ms = _millis(minute_end)
    if minute_end_ms - minute_start_ms != 60_000:
        raise ValueError('Latest Binance spot fetch requires exactly one closed minute.')

    start_trade_id = _first_trade_id_at_or_after(symbol, minute_start_ms)
    first_after_minute_id = _first_trade_id_at_or_after(symbol, minute_end_ms)
    end_trade_id = first_after_minute_id - 1
    if end_trade_id < start_trade_id:
        raise RuntimeError('Closed-minute Binance trade id range is empty.')

    rows = _fetch_trade_range(
        symbol,
        start_trade_id=start_trade_id,
        end_trade_id=end_trade_id,
    )
    _validate_closed_minute_batch(
        rows,
        start_trade_id=start_trade_id,
        end_trade_id=end_trade_id,
        minute_start_ms=minute_start_ms,
        minute_end_ms=minute_end_ms,
    )

    return LatestTradeBatch(
        bounds=LatestTradeIdBounds(
            minute_start=_utc_naive(minute_start),
            minute_end=_utc_naive(minute_end),
            start_trade_id=start_trade_id,
            end_trade_id=end_trade_id,
        ),
        rows=rows,
    )
