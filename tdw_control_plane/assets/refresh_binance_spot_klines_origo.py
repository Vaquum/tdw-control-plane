from datetime import datetime, timedelta, timezone

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from .create_binance_trades_table_origo import RAW_TABLE_NAME
from .create_binance_spot_klines_table_origo import (
    KLINES_TABLE_NAME,
    create_binance_spot_klines_table_origo,
)
from .create_origo_database import _get_clickhouse_settings, _make_clickhouse_client
from .daily_trades_to_origo import daily_partitions, insert_daily_binance_spot_trades_to_origo


def _partition_date_from_context(context: AssetExecutionContext) -> str:
    partition_key = context.partition_key
    if partition_key is not None:
        return partition_key

    target_date = datetime.now(timezone.utc) - timedelta(days=1)
    return target_date.strftime('%Y-%m-%d')


def _delete_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{KLINES_TABLE_NAME}
        DELETE WHERE toDate(fromUnixTimestamp64Milli(open_time)) = toDate('{partition_date}')
        """,
        settings={'mutations_sync': 2},
    )


def _count_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{KLINES_TABLE_NAME}
        WHERE toDate(fromUnixTimestamp64Milli(open_time)) = toDate('{partition_date}')
        """
    )
    return int(result[0][0])


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{KLINES_TABLE_NAME}
        SELECT
            toUnixTimestamp(toStartOfMinute(datetime)) * 1000 AS open_time,
            argMin(price, trade_id) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, trade_id) AS close,
            sum(quantity) AS volume,
            ((toUnixTimestamp(toStartOfMinute(datetime)) + 60) * 1000) - 1 AS close_time,
            sum(quote_quantity) AS quote_asset_volume,
            count() AS number_of_trades,
            sumIf(quantity, is_buyer_maker = 0) AS taker_buy_base_asset_volume,
            sumIf(quote_quantity, is_buyer_maker = 0) AS taker_buy_quote_asset_volume,
            toFloat64(0) AS ignore
        FROM {database}.{RAW_TABLE_NAME}
        WHERE toDate(datetime) = toDate('{partition_date}')
        GROUP BY toStartOfMinute(datetime)
        ORDER BY open_time
        """
    )


@asset(
    partitions_def=daily_partitions,
    deps=[
        create_binance_spot_klines_table_origo,
        insert_daily_binance_spot_trades_to_origo,
    ],
    group_name='binance_data',
    description='Refreshes the Binance spot 1m kline projection from source-native daily trades',
)
def refresh_binance_spot_klines_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date = _partition_date_from_context(context)
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing Binance spot kline rows for {partition_date}. '
                'Replacing that partition.'
            )
            _delete_partition_rows(client, settings.database, partition_date)

        _insert_partition_rows(client, settings.database, partition_date)
        inserted_count = _count_partition_rows(client, settings.database, partition_date)

        return {
            'status': 'success',
            'partition_date': partition_date,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
