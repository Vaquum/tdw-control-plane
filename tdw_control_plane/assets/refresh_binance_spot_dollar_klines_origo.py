from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, asset

from .create_binance_spot_dollar_klines_table_origo import (
    DOLLAR_KLINES_TABLE_NAME,
    create_binance_spot_dollar_klines_table_origo,
)
from .create_binance_trades_table_origo import RAW_TABLE_NAME
from .create_origo_database import (
    ClickHouseClientProtocol,
    _get_clickhouse_settings,
    _make_clickhouse_client,
)
from .daily_trades_to_origo import daily_partitions, insert_daily_binance_spot_trades_to_origo

DOLLAR_KLINE_SIZE = 100_000.0


def _partition_date_from_context(context: AssetExecutionContext) -> str:
    partition_key = context.partition_key
    if partition_key is not None:
        return partition_key

    target_date = datetime.now(UTC) - timedelta(days=1)
    return target_date.strftime('%Y-%m-%d')


def _delete_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{DOLLAR_KLINES_TABLE_NAME}
        DELETE WHERE toDate(start_datetime) = toDate('{partition_date}')
        """,
        settings={'mutations_sync': 2},
    )


def _count_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{DOLLAR_KLINES_TABLE_NAME}
        WHERE toDate(start_datetime) = toDate('{partition_date}')
        """
    )
    return int(result[0][0])


def _count_raw_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{RAW_TABLE_NAME}
        WHERE toDate(datetime) = toDate('{partition_date}')
        """
    )
    return int(result[0][0])


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{DOLLAR_KLINES_TABLE_NAME}
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
                FROM {database}.{RAW_TABLE_NAME}
                WHERE toDate(datetime) = toDate('{partition_date}')
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
    )


@asset(
    partitions_def=daily_partitions,
    deps=[
        create_binance_spot_dollar_klines_table_origo,
        insert_daily_binance_spot_trades_to_origo,
    ],
    group_name='binance_data',
    description=(
        'Refreshes the daily-scoped Binance spot dollar kline projection from '
        'source-native daily trades; dollar_bar_id resets each date and the final '
        'daily bar may be below the dollar threshold.'
    ),
)
def refresh_binance_spot_dollar_klines_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date = _partition_date_from_context(context)
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        raw_count = _count_raw_partition_rows(client, settings.database, partition_date)
        if raw_count == 0:
            raise RuntimeError(
                f'Raw Binance spot trades are missing for {partition_date}. '
                f'Run insert_daily_binance_spot_trades_to_origo for that partition first.'
            )

        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing Binance spot dollar kline rows for '
                f'{partition_date}. Replacing that partition.'
            )
            _delete_partition_rows(client, settings.database, partition_date)

        _insert_partition_rows(client, settings.database, partition_date)
        inserted_count = _count_partition_rows(client, settings.database, partition_date)

        return {
            'status': 'success',
            'partition_date': partition_date,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{DOLLAR_KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
