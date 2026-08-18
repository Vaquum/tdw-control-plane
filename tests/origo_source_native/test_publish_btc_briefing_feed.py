from __future__ import annotations

import importlib
import inspect
from datetime import date
from typing import Any, Callable

import pytest
from dagster import materialize

import origo.assets.publish_btc_briefing_feed as publish_btc_briefing_feed_module
from origo.assets.publish_btc_briefing_feed import (
    FEED_SECTIONS,
    FEED_VERSION,
    _make_clickhouse_arrow_client,
    build_briefing_feed,
)

DAY = date(2024, 1, 1)
TRADES_IN_DAY = 4800
SATS_PER_BTC = 100_000_000


def _create_briefing_tables(origo_assets: dict[str, Any]) -> None:
    result = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_daily_spot_trades_table_origo'],
            origo_assets['create_binance_spot_klines_table_origo'],
            origo_assets['create_binance_spot_depth20_1m_table_origo'],
        ]
    )
    assert result.success


def _insert_minute_klines(query_origo: Callable[[str], list[tuple[Any, ...]]], minutes: int) -> None:
    query_origo(
        f"""
        INSERT INTO binance_spot_klines
            (datetime, open, high, low, close, mean, std, median, iqr, volume,
             maker_ratio, no_of_trades, open_liquidity, high_liquidity, low_liquidity,
             close_liquidity, liquidity_sum, maker_volume, maker_liquidity)
        SELECT
            toDateTime('{DAY.isoformat()} 00:00:00') + 60 * number AS datetime,
            42000 + (number % 96) AS open,
            open + 5 AS high,
            open - 5 AS low,
            open + 2 AS close,
            open AS mean,
            1.5 AS std,
            open AS median,
            1.0 AS iqr,
            2.5 AS volume,
            0.5 AS maker_ratio,
            10 AS no_of_trades,
            100.0 AS open_liquidity,
            110.0 AS high_liquidity,
            90.0 AS low_liquidity,
            105.0 AS close_liquidity,
            1000.0 AS liquidity_sum,
            1.25 AS maker_volume,
            500.0 AS maker_liquidity
        FROM numbers({minutes})
        """
    )


def _insert_day_trades(query_origo: Callable[[str], list[tuple[Any, ...]]]) -> None:
    # Quantities carry a live 8th decimal so the satoshi conversion is
    # exercised at its full precision, not only on round numbers.
    query_origo(
        f"""
        INSERT INTO binance_daily_spot_trades
            (trade_id, price, quantity, quote_quantity, timestamp,
             is_buyer_maker, is_best_match, datetime)
        SELECT
            number AS trade_id,
            42000 + (number % 50) * 0.01 AS price,
            0.001 + (number % 7) * 0.00000001 AS quantity,
            price * quantity AS quote_quantity,
            number AS timestamp,
            number % 3 = 0 AS is_buyer_maker,
            1 AS is_best_match,
            toDateTime64('{DAY.isoformat()} 00:00:00', 6) + 18 * number AS datetime
        FROM numbers({TRADES_IN_DAY})
        """
    )


def _insert_day_book_minutes(query_origo: Callable[[str], list[tuple[Any, ...]]]) -> None:
    query_origo(
        f"""
        INSERT INTO binance_spot_depth20_1m
            (datetime, source_timestamp_ms, book_mid_price, book_spread_bps,
             book_bid_depth_20_notional, book_ask_depth_20_notional, book_imbalance_20)
        SELECT
            toDateTime('{DAY.isoformat()} 00:00:00') + 60 * number AS datetime,
            toUnixTimestamp(datetime) * 1000 AS source_timestamp_ms,
            42000 + (number % 60) AS book_mid_price,
            1 + (number % 10) / 10 AS book_spread_bps,
            1000000 + 1000 * (number % 20) AS book_bid_depth_20_notional,
            1000000 - 1000 * (number % 20) AS book_ask_depth_20_notional,
            (book_bid_depth_20_notional - book_ask_depth_20_notional)
              / (book_bid_depth_20_notional + book_ask_depth_20_notional) AS book_imbalance_20
        FROM numbers(1440)
        """
    )


def _build_feed_for_day(day: date) -> dict[str, Any]:
    client = _make_clickhouse_arrow_client()
    try:
        return build_briefing_feed(client, day)
    finally:
        client.close()


def test_build_briefing_feed_signature_is_client_and_day() -> None:
    signature = inspect.signature(build_briefing_feed)
    assert list(signature.parameters) == ['client', 'day']
    assert (
        signature.parameters['client'].annotation
        is publish_btc_briefing_feed_module._ClickHouseArrowClientProtocol
    )
    assert signature.parameters['day'].annotation is date
    assert signature.return_annotation == dict[str, object]


def test_feed_version_is_pinned() -> None:
    assert FEED_VERSION == 'btc_briefing/1'


def test_feed_sections_are_exactly_the_six_published() -> None:
    assert sorted(FEED_SECTIONS) == [
        'bars_15m',
        'bars_1d',
        'book_percentiles',
        'book_series',
        'book_sessions',
        'volume_at_price',
    ]


def test_feed_contains_every_declared_section(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1440)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    feed = _build_feed_for_day(DAY)

    assert feed['feed_version'] == FEED_VERSION
    assert feed['day'] == DAY.isoformat()
    for section in FEED_SECTIONS:
        assert section in feed
        assert len(feed[section]) > 0


def test_bars_cover_the_day_with_no_short_bar(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1440)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    feed = _build_feed_for_day(DAY)

    assert len(feed['bars_15m']) == 96
    assert all(bar['source_minutes'] == 15 for bar in feed['bars_15m'])
    assert len(feed['bars_1d']) == 1
    assert feed['bars_1d'][0]['source_minutes'] == 1440


def test_volume_at_price_conserves_in_integer_units(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1440)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    feed = _build_feed_for_day(DAY)
    volume_at_price = feed['volume_at_price']

    for row in volume_at_price:
        assert isinstance(row['taker_buy_sats'], int)
        assert isinstance(row['taker_sell_sats'], int)
        assert isinstance(row['total_sats'], int)
        assert row['taker_buy_sats'] + row['taker_sell_sats'] == row['total_sats']

    expected_total_sats = query_origo(
        f'SELECT sum(toUInt64(round(quantity * {SATS_PER_BTC}))) FROM binance_daily_spot_trades'
    )[0][0]
    assert sum(row['total_sats'] for row in volume_at_price) == expected_total_sats
    assert sum(row['trades'] for row in volume_at_price) == TRADES_IN_DAY


def test_incomplete_day_raises(
    origo_assets: dict[str, Any],
    query_origo: Callable[[str], list[tuple[Any, ...]]],
) -> None:
    _create_briefing_tables(origo_assets)
    _insert_minute_klines(query_origo, 1439)
    _insert_day_trades(query_origo)
    _insert_day_book_minutes(query_origo)

    with pytest.raises(RuntimeError, match='refusing to build a short briefing feed'):
        _build_feed_for_day(DAY)


def test_job_is_registered_in_definitions() -> None:
    definitions_module = importlib.import_module('origo.definitions')
    assert 'publish_btc_briefing_feed_job' in [
        job.name for job in definitions_module.defs.jobs
    ]
