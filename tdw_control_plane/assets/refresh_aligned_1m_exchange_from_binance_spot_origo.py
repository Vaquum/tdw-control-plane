from datetime import datetime, timedelta, timezone

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from .create_aligned_1m_exchange_table_origo import (
    ALIGNED_TABLE_NAME,
    create_aligned_1m_exchange_table_origo,
)
from .create_binance_spot_klines_table_origo import (
    KLINES_TABLE_NAME,
    create_binance_spot_klines_table_origo,
)
from .create_origo_database import _get_clickhouse_settings, _make_clickhouse_client
from .daily_trades_to_origo import daily_partitions
from .refresh_binance_spot_klines_origo import refresh_binance_spot_klines_origo

BINANCE_SPOT_DATASET_SOURCE = 'binance_spot'


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
        ALTER TABLE {database}.{ALIGNED_TABLE_NAME}
        DELETE WHERE dataset_source = '{BINANCE_SPOT_DATASET_SOURCE}'
          AND toDate(fromUnixTimestamp64Milli(open_time)) = toDate('{partition_date}')
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
        FROM {database}.{ALIGNED_TABLE_NAME}
        WHERE dataset_source = '{BINANCE_SPOT_DATASET_SOURCE}'
          AND toDate(fromUnixTimestamp64Milli(open_time)) = toDate('{partition_date}')
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
        INSERT INTO {database}.{ALIGNED_TABLE_NAME}
        SELECT
            '{BINANCE_SPOT_DATASET_SOURCE}' AS dataset_source,
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            quote_asset_volume,
            number_of_trades,
            taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume,
            ignore
        FROM {database}.{KLINES_TABLE_NAME}
        WHERE toDate(fromUnixTimestamp64Milli(open_time)) = toDate('{partition_date}')
        ORDER BY open_time
        """
    )


@asset(
    partitions_def=daily_partitions,
    deps=[
        create_aligned_1m_exchange_table_origo,
        create_binance_spot_klines_table_origo,
        refresh_binance_spot_klines_origo,
    ],
    group_name='binance_data',
    description='Refreshes the shared aligned_1m_exchange dataset with Binance spot rows',
)
def refresh_aligned_1m_exchange_from_binance_spot_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date = _partition_date_from_context(context)
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} aligned Binance spot rows for {partition_date}. '
                'Replacing that partition.'
            )
            _delete_partition_rows(client, settings.database, partition_date)

        _insert_partition_rows(client, settings.database, partition_date)
        inserted_count = _count_partition_rows(client, settings.database, partition_date)

        return {
            'status': 'success',
            'partition_date': partition_date,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{ALIGNED_TABLE_NAME}',
            'dataset_source': BINANCE_SPOT_DATASET_SOURCE,
        }
    finally:
        client.disconnect()
