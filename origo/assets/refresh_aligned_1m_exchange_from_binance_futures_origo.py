from datetime import date, datetime, timedelta, timezone

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from .create_aligned_1m_exchange_table_origo import (
    ALIGNED_TABLE_NAME,
    create_aligned_1m_exchange_table_origo,
)
from .create_binance_futures_klines_table_origo import (
    KLINES_TABLE_NAME,
    create_binance_futures_klines_table_origo,
)
from .create_origo_database import _get_clickhouse_settings, _make_clickhouse_client
from .daily_futures_trades_to_origo import daily_partitions
from .refresh_binance_futures_klines_origo import refresh_binance_futures_klines_origo

BINANCE_FUTURES_DATASET_SOURCE = 'binance_futures'


def _partition_date_from_context(context: AssetExecutionContext) -> str:
    partition_key = context.partition_key
    if partition_key:
        return date.fromisoformat(partition_key).isoformat()

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
        DELETE WHERE dataset_source = '{BINANCE_FUTURES_DATASET_SOURCE}'
          AND toDate(datetime) = toDate('{partition_date}')
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
        WHERE dataset_source = '{BINANCE_FUTURES_DATASET_SOURCE}'
          AND toDate(datetime) = toDate('{partition_date}')
        """
    )
    if not isinstance(result, list) or not result:
        raise TypeError(f'Expected row result from ClickHouse, got {type(result).__name__}')

    row = result[0]
    if not isinstance(row, tuple) or not row:
        raise TypeError(f'Expected tuple row from ClickHouse, got {type(row).__name__}')

    value = row[0]
    if not isinstance(value, int):
        raise TypeError(f'Expected int scalar from ClickHouse, got {type(value).__name__}')

    return value


def _insert_partition_rows(
    client: ClickhouseClient,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{ALIGNED_TABLE_NAME}
        SELECT
            '{BINANCE_FUTURES_DATASET_SOURCE}' AS dataset_source,
            datetime,
            open,
            high,
            low,
            close,
            mean,
            std,
            median,
            iqr,
            volume,
            maker_ratio,
            no_of_trades,
            open_liquidity,
            high_liquidity,
            low_liquidity,
            close_liquidity,
            liquidity_sum,
            maker_volume,
            maker_liquidity
        FROM {database}.{KLINES_TABLE_NAME}
        WHERE toDate(datetime) = toDate('{partition_date}')
        ORDER BY datetime
        """
    )


@asset(
    partitions_def=daily_partitions,
    deps=[
        create_aligned_1m_exchange_table_origo,
        create_binance_futures_klines_table_origo,
        refresh_binance_futures_klines_origo,
    ],
    group_name='binance_futures_data',
    description='Refreshes the shared aligned_1m_exchange dataset with Binance futures rows',
)
def refresh_aligned_1m_exchange_from_binance_futures_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date = _partition_date_from_context(context)
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} aligned Binance futures rows for {partition_date}. '
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
            'dataset_source': BINANCE_FUTURES_DATASET_SOURCE,
        }
    finally:
        client.disconnect()
