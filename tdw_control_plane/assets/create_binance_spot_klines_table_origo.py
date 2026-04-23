from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

from .create_origo_database import (
    ClickHouseSettings,
    _get_clickhouse_settings,
    _make_clickhouse_client,
    create_origo_database,
)

KLINES_TABLE_NAME = 'binance_spot_klines'


def _database_exists(client: ClickhouseClient, database: str) -> bool:
    result = client.execute(
        f"""
        SELECT count()
        FROM system.databases
        WHERE name = '{database}'
        """
    )
    return bool(result[0][0])


def _table_exists(client: ClickhouseClient, settings: ClickHouseSettings, table_name: str) -> bool:
    result = client.execute(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{settings.database}'
          AND name = '{table_name}'
        """
    )
    return bool(result[0][0])


def _create_klines_table(client: ClickhouseClient, settings: ClickHouseSettings) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{KLINES_TABLE_NAME} (
            open_time UInt64,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            close_time UInt64,
            quote_asset_volume Float64,
            number_of_trades UInt64,
            taker_buy_base_asset_volume Float64,
            taker_buy_quote_asset_volume Float64,
            ignore Float64
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(open_time))
        ORDER BY open_time
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description='Creates the binance_spot_klines table if it does not exist',
)
def create_binance_spot_klines_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        if not _database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        table_existed = _table_exists(client, settings, KLINES_TABLE_NAME)
        _create_klines_table(client, settings)

        context.log.info(f'Ensured table {settings.database}.{KLINES_TABLE_NAME} exists.')
        return {
            'status': 'success',
            'table': f'{settings.database}.{KLINES_TABLE_NAME}',
            'table_action': 'already_exists' if table_existed else 'created',
        }
    finally:
        client.disconnect()
