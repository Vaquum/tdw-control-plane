from datetime import datetime, timedelta

from dagster import AssetExecutionContext, asset

from .create_binance_spot_latest_tables_origo import (
    LATEST_INGESTION_TABLE_NAME,
    LATEST_KLINES_TABLE_NAME,
    LATEST_RAW_TABLE_NAME,
    create_binance_spot_latest_tables_origo,
)
from .create_origo_database import (
    ClickHouseClientProtocol,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .sync_binance_spot_trades_latest_origo import (
    latest_minute_from_context,
    sync_binance_spot_trades_latest_origo,
)


def _minute_bounds(minute_start: datetime) -> tuple[str, str]:
    minute_end = minute_start + timedelta(minutes=1)
    return (
        minute_start.strftime('%Y-%m-%d %H:%M:%S'),
        minute_end.strftime('%Y-%m-%d %H:%M:%S'),
    )


def _single_int_result(result: list[tuple[object, ...]], field_name: str) -> int:
    value = result[0][0]
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f'{field_name} query must return an integer.')
    return value


def ensure_latest_trades_ingested(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{LATEST_INGESTION_TABLE_NAME}
        WHERE minute_start = toDateTime('{minute_start:%Y-%m-%d %H:%M:%S}')
          AND status = 'success'
        """
    )
    if _single_int_result(result, 'latest ingestion count') != 1:
        raise RuntimeError(
            f'Latest Binance spot trades for {minute_start:%Y-%m-%d %H:%M:%S} are not ingested.'
        )


def _delete_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        ALTER TABLE {database}.{LATEST_KLINES_TABLE_NAME}
        DELETE WHERE datetime >= toDateTime('{start_datetime}')
          AND datetime < toDateTime('{end_datetime}')
        """,
        settings={'mutations_sync': 2},
    )


def _count_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> int:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{LATEST_KLINES_TABLE_NAME}
        WHERE datetime >= toDateTime('{start_datetime}')
          AND datetime < toDateTime('{end_datetime}')
        """
    )
    return _single_int_result(result, 'latest kline count')


def _insert_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        INSERT INTO {database}.{LATEST_KLINES_TABLE_NAME}
        SELECT
            kline_datetime AS datetime,
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
            FROM {database}.{LATEST_RAW_TABLE_NAME}
            WHERE datetime >= toDateTime64('{start_datetime}', 3)
              AND datetime < toDateTime64('{end_datetime}', 3)
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )


@asset(
    deps=[
        create_binance_spot_latest_tables_origo,
        sync_binance_spot_trades_latest_origo,
    ],
    group_name='binance_data',
    description='Refreshes the rolling latest Binance spot 1m kline foundation.',
)
def refresh_binance_spot_klines_latest_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    minute_start = latest_minute_from_context(context)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        ensure_latest_trades_ingested(client, settings.database, minute_start)
        _delete_minute_rows(client, settings.database, minute_start)
        _insert_minute_rows(client, settings.database, minute_start)
        inserted_count = _count_minute_rows(client, settings.database, minute_start)
        return {
            'status': 'success',
            'minute_start': minute_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{LATEST_KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
