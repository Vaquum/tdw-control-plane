import os
from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, asset

from origo.utils.binance_spot_latest import (
    BinanceHistoricalTrade,
    LatestTradeBatch,
    fetch_closed_minute_trades,
)

from .create_binance_spot_latest_tables_origo import (
    LATEST_INGESTION_TABLE_NAME,
    LATEST_RAW_TABLE_NAME,
    LATEST_WATERMARKS_TABLE_NAME,
    create_binance_spot_latest_tables_origo,
)
from .create_origo_database import (
    ClickHouseClientProtocol,
    get_clickhouse_settings,
    make_clickhouse_client,
)

LATEST_MINUTE_START_TAG = 'binance_spot_latest_minute_start'
LATEST_SYMBOL_ENV_VAR = 'BINANCE_SPOT_LATEST_SYMBOL'
LATEST_SYMBOL_DEFAULT = 'BTCUSDT'
LATEST_WATERMARK_LAYER_TRADES = 'trades'


def _minute_key(value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_minute_key(value: str) -> datetime:
    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=UTC)


def _last_completed_minute() -> datetime:
    return datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)


def latest_minute_from_context(context: AssetExecutionContext) -> datetime:
    value = context.run.tags.get(LATEST_MINUTE_START_TAG)
    if value is not None:
        return _parse_minute_key(value)
    return _last_completed_minute()


def _delete_latest_minute(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{LATEST_RAW_TABLE_NAME}
        DELETE WHERE minute_start = toDateTime('{minute_start:%Y-%m-%d %H:%M:%S}')
        """,
        settings={'mutations_sync': 2},
    )


def _delete_ingestion_minute(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{LATEST_INGESTION_TABLE_NAME}
        DELETE WHERE minute_start = toDateTime('{minute_start:%Y-%m-%d %H:%M:%S}')
        """,
        settings={'mutations_sync': 2},
    )


def _insert_latest_rows(
    client: ClickHouseClientProtocol,
    database: str,
    *,
    minute_start: datetime,
    rows: tuple[BinanceHistoricalTrade, ...],
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{LATEST_RAW_TABLE_NAME}
        (
            minute_start,
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            is_best_match,
            datetime
        ) VALUES
        """,
        [
            (
                minute_start.replace(tzinfo=None),
                row.trade_id,
                row.price,
                row.quantity,
                row.quote_quantity,
                row.timestamp,
                row.is_buyer_maker,
                row.is_best_match,
                row.datetime,
            )
            for row in rows
        ],
    )


def _write_success_ingestion(
    client: ClickHouseClientProtocol,
    database: str,
    context: AssetExecutionContext,
    batch: LatestTradeBatch,
) -> None:
    minute_start = batch.bounds.minute_start
    _delete_ingestion_minute(client, database, minute_start)
    client.execute(
        f"""
        INSERT INTO {database}.{LATEST_INGESTION_TABLE_NAME}
        (
            minute_start,
            start_trade_id,
            end_trade_id,
            row_count,
            status,
            dagster_run_id,
            loaded_at
        ) VALUES
        """,
        [
            (
                minute_start,
                batch.bounds.start_trade_id,
                batch.bounds.end_trade_id,
                len(batch.rows),
                'success',
                context.run.run_id,
                datetime.now(UTC).replace(tzinfo=None, microsecond=0),
            )
        ],
    )


def _latest_watermark(
    client: ClickHouseClientProtocol,
    database: str,
    layer: str,
) -> datetime | None:
    result = client.execute(
        f"""
        SELECT watermark_minute
        FROM {database}.{LATEST_WATERMARKS_TABLE_NAME}
        WHERE layer = '{layer}'
        ORDER BY updated_at DESC
        LIMIT 1
        """
    )
    if len(result) == 0:
        return None
    value = result[0][0]
    if not isinstance(value, datetime):
        raise RuntimeError(f'Watermark for {layer} must be a datetime.')
    return value


def _successful_ingestion_minutes_after(
    client: ClickHouseClientProtocol,
    database: str,
    watermark: datetime | None,
) -> tuple[datetime, ...]:
    if watermark is None:
        predicate = '1 = 1'
    else:
        predicate = f"minute_start > toDateTime('{watermark:%Y-%m-%d %H:%M:%S}')"

    result = client.execute(
        f"""
        SELECT DISTINCT minute_start
        FROM {database}.{LATEST_INGESTION_TABLE_NAME}
        WHERE status = 'success'
          AND {predicate}
        ORDER BY minute_start
        """
    )
    minutes: list[datetime] = []
    for row in result:
        value = row[0]
        if not isinstance(value, datetime):
            raise RuntimeError('Successful ingestion minute must be a datetime.')
        minutes.append(value)
    return tuple(minutes)


def _replace_watermark(
    client: ClickHouseClientProtocol,
    database: str,
    layer: str,
    watermark: datetime,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{LATEST_WATERMARKS_TABLE_NAME}
        DELETE WHERE layer = '{layer}'
        """,
        settings={'mutations_sync': 2},
    )
    client.execute(
        f"""
        INSERT INTO {database}.{LATEST_WATERMARKS_TABLE_NAME}
        (layer, watermark_minute, updated_at)
        VALUES
        """,
        [
            (
                layer,
                watermark,
                datetime.now(UTC).replace(tzinfo=None, microsecond=0),
            )
        ],
    )


def advance_latest_watermark(
    client: ClickHouseClientProtocol,
    database: str,
    layer: str,
) -> datetime | None:
    current = _latest_watermark(client, database, layer)
    successful_minutes = _successful_ingestion_minutes_after(client, database, current)
    if not successful_minutes:
        return current

    if current is None:
        advanced = successful_minutes[0]
        expected = advanced + timedelta(minutes=1)
        remaining_minutes = successful_minutes[1:]
    else:
        advanced = current
        expected = current + timedelta(minutes=1)
        remaining_minutes = successful_minutes

    for minute_start in remaining_minutes:
        if minute_start == expected:
            advanced = minute_start
            expected = minute_start + timedelta(minutes=1)
        elif minute_start > expected:
            break

    if current != advanced:
        _replace_watermark(client, database, layer, advanced)
    return advanced


@asset(
    deps=[create_binance_spot_latest_tables_origo],
    group_name='binance_data',
    description='Fetches and stores the exact closed-minute Binance spot trade id range.',
)
def sync_binance_spot_trades_latest_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    minute_start = latest_minute_from_context(context)
    minute_end = minute_start + timedelta(minutes=1)
    symbol = os.environ.get(LATEST_SYMBOL_ENV_VAR, LATEST_SYMBOL_DEFAULT)
    batch = fetch_closed_minute_trades(symbol, minute_start, minute_end)

    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)
    try:
        _delete_latest_minute(client, settings.database, batch.bounds.minute_start)
        _insert_latest_rows(
            client,
            settings.database,
            minute_start=batch.bounds.minute_start,
            rows=batch.rows,
        )
        _write_success_ingestion(client, settings.database, context, batch)
        watermark = advance_latest_watermark(
            client,
            settings.database,
            LATEST_WATERMARK_LAYER_TRADES,
        )
        return {
            'status': 'success',
            'symbol': symbol,
            'minute_start': _minute_key(batch.bounds.minute_start),
            'start_trade_id': batch.bounds.start_trade_id,
            'end_trade_id': batch.bounds.end_trade_id,
            'rows_inserted': len(batch.rows),
            'watermark_minute': _minute_key(watermark) if watermark is not None else None,
        }
    finally:
        client.disconnect()
