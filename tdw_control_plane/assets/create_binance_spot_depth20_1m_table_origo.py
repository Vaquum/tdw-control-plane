from dagster import AssetExecutionContext, asset

from .create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseSettings,
    ClickHouseClient,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .create_origo_database import create_origo_database

DEPTH20_1M_TABLE_NAME = 'binance_spot_depth20_1m'


def _create_depth20_1m_table(client: ClickHouseClient, settings: ClickHouseSettings) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{DEPTH20_1M_TABLE_NAME} (
            datetime DateTime,
            book_mid_price Float64,
            book_spread_bps Float64,
            book_bid_depth_20_notional Float64,
            book_ask_depth_20_notional Float64,
            book_imbalance_20 Float64
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(datetime)
        ORDER BY datetime
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description='Creates the binance_spot_depth20_1m table if it does not exist',
)
def create_binance_spot_depth20_1m_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        _create_depth20_1m_table(client, settings)

        context.log.info(f'Ensured table {settings.database}.{DEPTH20_1M_TABLE_NAME} exists.')
        return {
            'status': 'success',
            'table': f'{settings.database}.{DEPTH20_1M_TABLE_NAME}',
        }
    finally:
        client.disconnect()
