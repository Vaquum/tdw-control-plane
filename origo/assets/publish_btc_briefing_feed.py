"""Build the daily BTC briefing feed from the origo ClickHouse tables.

The feed is one dict per UTC day carrying the six sections in
``FEED_SECTIONS``: 15-minute and daily OHLCV bars from the 1m
``binance_spot_klines`` projection, measured volume-at-price from
``binance_daily_spot_trades``, and per-minute series, exact daily
percentiles and 8-hour session aggregates of the ``binance_spot_depth20_1m``
book projection.

Completeness policy: bars must cover the day exactly (96 x 15 distinct
source minutes, 1 x 1440, with no duplicated 1m row) and every section must
be non-empty, otherwise the build raises instead of returning a short or
double-counted feed. Book minute coverage below
1440 is carried visibly in the section rows rather than rejected, because
the per-minute book capture is best-effort and a single missed minute must
not suppress the whole feed.

Every time field the sections carry (``bar_start``, ``minute_start``,
``session_start``) is declared as UTC epoch seconds in the SQL itself via
``toUnixTimestamp``, so the feed's time representation is part of the
``btc_briefing/1`` contract rather than inherited from the server's Arrow
serialization of ``DateTime``.

The asset only builds and validates the feed; delivery is a separate slice.
"""

import os
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Final, Protocol, cast

from dagster import AssetExecutionContext, asset

FEED_VERSION: Final[str] = 'btc_briefing/1'
FEED_SECTIONS: Final[tuple[str, ...]] = (
    'bars_15m',
    'bars_1d',
    'volume_at_price',
    'book_percentiles',
    'book_series',
    'book_sessions',
)

BARS_15M_SECONDS = 900
BARS_1D_SECONDS = 86400
MINUTES_PER_DAY = 1440
BARS_15M_PER_DAY = 96
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123

_SQL_DIR = Path(__file__).parent / 'sql'


class _ArrowTableProtocol(Protocol):
    def to_pylist(self) -> list[dict[str, object]]:
        raise NotImplementedError


class _ClickHouseArrowClientProtocol(Protocol):
    def query_arrow(
        self,
        query: str,
        parameters: Mapping[str, object] | None = None,
    ) -> _ArrowTableProtocol:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def _get_clickhouse_http_port() -> int:
    value = os.environ.get('CLICKHOUSE_HTTP_PORT', str(DEFAULT_CLICKHOUSE_HTTP_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_HTTP_PORT environment variable must be an integer.') from exc


def _make_clickhouse_arrow_client() -> _ClickHouseArrowClientProtocol:
    client_factory = getattr(import_module('clickhouse_connect'), 'get_client')
    return cast(
        _ClickHouseArrowClientProtocol,
        client_factory(
            host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
            port=_get_clickhouse_http_port(),
            username=os.environ.get('CLICKHOUSE_USER', 'default'),
            password=os.environ['CLICKHOUSE_PASSWORD'],
        ),
    )


def _section_rows(
    client: _ClickHouseArrowClientProtocol,
    sql_file_name: str,
    parameters: Mapping[str, object],
) -> list[dict[str, object]]:
    arrow_table = client.query_arrow(
        (_SQL_DIR / sql_file_name).read_text(encoding='utf-8'),
        parameters=parameters,
    )
    return arrow_table.to_pylist()


def _require_complete_bars(
    bars: list[dict[str, object]],
    *,
    day: date,
    section: str,
    expected_bars: int,
    minutes_per_bar: int,
) -> None:
    if len(bars) != expected_bars:
        raise RuntimeError(
            f'{day.isoformat()} has {len(bars)} {section} bars where {expected_bars} '
            'are required; refusing to build a short briefing feed.'
        )
    short_bars = [bar for bar in bars if bar['source_minutes'] != minutes_per_bar]
    if short_bars:
        raise RuntimeError(
            f'{day.isoformat()} has {len(short_bars)} {section} bars not built from '
            f'exactly {minutes_per_bar} distinct source minutes; refusing to build '
            'a short briefing feed.'
        )
    duplicated_bars = [bar for bar in bars if bar['source_rows'] != bar['source_minutes']]
    if duplicated_bars:
        raise RuntimeError(
            f'{day.isoformat()} has {len(duplicated_bars)} {section} bars with '
            'duplicated 1m source rows; refusing to build a briefing feed from a '
            'corrupt projection.'
        )


def _require_rows(rows: list[dict[str, object]], *, day: date, section: str) -> None:
    if not rows:
        raise RuntimeError(
            f'{day.isoformat()} returned no {section} rows; refusing to build an '
            'empty briefing section.'
        )


def build_briefing_feed(
    client: _ClickHouseArrowClientProtocol, day: date
) -> dict[str, object]:
    """Build the ``btc_briefing/1`` feed dict for one complete UTC day.

    The dict carries ``feed_version``, ``day`` and every section in
    ``FEED_SECTIONS``, each section a list of row dicts straight from its
    SQL file in ``origo/assets/sql/``. Raises if the day's bars are not
    complete (96 x 15 source minutes, 1 x 1440) or any section comes back
    empty, so a short day can never masquerade as a published feed.
    """
    bars_15m = _section_rows(
        client, 'briefing_bars.sql', {'day': day, 'bucket_seconds': BARS_15M_SECONDS}
    )
    _require_complete_bars(
        bars_15m,
        day=day,
        section='bars_15m',
        expected_bars=BARS_15M_PER_DAY,
        minutes_per_bar=BARS_15M_SECONDS // 60,
    )

    bars_1d = _section_rows(
        client, 'briefing_bars.sql', {'day': day, 'bucket_seconds': BARS_1D_SECONDS}
    )
    _require_complete_bars(
        bars_1d,
        day=day,
        section='bars_1d',
        expected_bars=1,
        minutes_per_bar=MINUTES_PER_DAY,
    )

    sections: dict[str, list[dict[str, object]]] = {
        'bars_15m': bars_15m,
        'bars_1d': bars_1d,
        'volume_at_price': _section_rows(
            client, 'briefing_volume_at_price.sql', {'day': day}
        ),
        'book_percentiles': _section_rows(
            client, 'briefing_book_percentiles.sql', {'day': day}
        ),
        'book_series': _section_rows(client, 'briefing_book_series.sql', {'day': day}),
        'book_sessions': _section_rows(
            client, 'briefing_book_sessions.sql', {'day': day}
        ),
    }
    for section_name, section_rows in sections.items():
        _require_rows(section_rows, day=day, section=section_name)

    return {
        'feed_version': FEED_VERSION,
        'day': day.isoformat(),
        **sections,
    }


@asset(
    name='publish_btc_briefing_feed',
    group_name='binance_data',
    description=(
        'Builds and validates the daily BTC briefing feed (btc_briefing/1) from the '
        'origo ClickHouse tables for the last complete UTC day.'
    ),
)
def publish_btc_briefing_feed(context: AssetExecutionContext) -> None:
    day = datetime.now(UTC).date() - timedelta(days=1)
    client = _make_clickhouse_arrow_client()
    try:
        feed = build_briefing_feed(client, day)
    finally:
        client.close()

    section_counts = {
        section_name: len(cast(list[dict[str, object]], feed[section_name]))
        for section_name in FEED_SECTIONS
    }
    context.log.info(f'Built {FEED_VERSION} feed for {day.isoformat()}: {section_counts}.')
