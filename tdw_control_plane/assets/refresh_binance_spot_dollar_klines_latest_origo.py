from datetime import datetime, timedelta

from dagster import AssetExecutionContext, asset

from .create_binance_spot_latest_tables_origo import (
    LATEST_DOLLAR_KLINES_TABLE_NAME,
    LATEST_RAW_TABLE_NAME,
    create_binance_spot_latest_tables_origo,
)
from .create_origo_database import (
    ClickHouseClientProtocol,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .refresh_binance_spot_dollar_klines_origo import DOLLAR_KLINE_SIZE
from .refresh_binance_spot_klines_latest_origo import ensure_latest_trades_ingested
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


def _delete_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        ALTER TABLE {database}.{LATEST_DOLLAR_KLINES_TABLE_NAME}
        DELETE WHERE start_datetime >= toDateTime('{start_datetime}')
          AND start_datetime < toDateTime('{end_datetime}')
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
        FROM {database}.{LATEST_DOLLAR_KLINES_TABLE_NAME}
        WHERE start_datetime >= toDateTime('{start_datetime}')
          AND start_datetime < toDateTime('{end_datetime}')
        """
    )
    return _single_int_result(result, 'latest dollar kline count')


def _insert_minute_rows(
    client: ClickHouseClientProtocol,
    database: str,
    minute_start: datetime,
) -> None:
    start_datetime, end_datetime = _minute_bounds(minute_start)
    client.execute(
        f"""
        INSERT INTO {database}.{LATEST_DOLLAR_KLINES_TABLE_NAME}
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
                toUInt64(floor(running_quote_before / {DOLLAR_KLINE_SIZE})) AS dollar_bar_id
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
                FROM {database}.{LATEST_RAW_TABLE_NAME}
                WHERE datetime >= toDateTime64('{start_datetime}', 3)
                  AND datetime < toDateTime64('{end_datetime}', 3)
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
    )


@asset(
    deps=[
        create_binance_spot_latest_tables_origo,
        sync_binance_spot_trades_latest_origo,
    ],
    group_name='binance_data',
    description='Refreshes the rolling latest Binance spot 1M dollar kline foundation.',
)
def refresh_binance_spot_dollar_klines_latest_origo(
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
            'table': f'{settings.database}.{LATEST_DOLLAR_KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
