from dagster import AssetExecutionContext, asset

from .create_binance_trades_table_origo import database_exists, table_exists
from .create_origo_database import (
    ClickHouseClientProtocol,
    ClickHouseSettings,
    create_origo_database,
    get_clickhouse_settings,
    make_clickhouse_client,
)

TICK_KLINES_TABLE_NAME = 'binance_spot_tick_klines'


def _create_tick_klines_table(
    client: ClickHouseClientProtocol,
    settings: ClickHouseSettings,
) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{TICK_KLINES_TABLE_NAME} (
            start_datetime DateTime,
            end_datetime DateTime,
            tick_bar_id UInt64 COMMENT 'Partition-date scoped id; resets each day and groups fixed trade counts.',
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            mean Float64,
            std Float64,
            median Float64,
            iqr Float64,
            volume Float64,
            maker_ratio Float64,
            no_of_trades UInt64,
            open_liquidity Float64,
            high_liquidity Float64,
            low_liquidity Float64,
            close_liquidity Float64,
            liquidity_sum Float64,
            maker_volume Float64,
            maker_liquidity Float64
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(start_datetime)
        ORDER BY (start_datetime, end_datetime, tick_bar_id)
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description=(
        'Creates the binance_spot_tick_klines table if it does not exist. '
        'tick_bar_id is scoped to each partition date.'
    ),
)
def create_binance_spot_tick_klines_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        if not database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        table_existed = table_exists(client, settings, TICK_KLINES_TABLE_NAME)
        _create_tick_klines_table(client, settings)

        context.log.info(f'Ensured table {settings.database}.{TICK_KLINES_TABLE_NAME} exists.')
        return {
            'status': 'success',
            'table': f'{settings.database}.{TICK_KLINES_TABLE_NAME}',
            'table_action': 'already_exists' if table_existed else 'created',
        }
    finally:
        client.disconnect()
