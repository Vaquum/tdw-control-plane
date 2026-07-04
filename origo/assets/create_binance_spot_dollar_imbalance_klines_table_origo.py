from dagster import AssetExecutionContext, asset

from .create_binance_trades_table_origo import database_exists, table_exists
from .create_origo_database import (
    ClickHouseClientProtocol,
    ClickHouseSettings,
    create_origo_database,
    get_clickhouse_settings,
    make_clickhouse_client,
)

DOLLAR_IMBALANCE_KLINES_TABLE_NAME = 'binance_spot_dollar_imbalance_klines'


def _create_dollar_imbalance_klines_table(
    client: ClickHouseClientProtocol,
    settings: ClickHouseSettings,
) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME} (
            start_datetime DateTime,
            end_datetime DateTime,
            dollar_imbalance_bar_id UInt64 COMMENT 'Partition-date scoped id; resets each day and groups fixed signed taker dollar imbalance thresholds.',
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
            maker_liquidity Float64,
            taker_buy_liquidity Float64,
            taker_sell_liquidity Float64,
            dollar_imbalance Float64
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(start_datetime)
        ORDER BY (start_datetime, end_datetime, dollar_imbalance_bar_id)
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description=(
        'Creates the binance_spot_dollar_imbalance_klines table if it does not exist. '
        'dollar_imbalance_bar_id is scoped to each partition date.'
    ),
)
def create_binance_spot_dollar_imbalance_klines_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        if not database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        table_existed = table_exists(client, settings, DOLLAR_IMBALANCE_KLINES_TABLE_NAME)
        _create_dollar_imbalance_klines_table(client, settings)

        context.log.info(
            f'Ensured table {settings.database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME} exists.'
        )
        return {
            'status': 'success',
            'table': f'{settings.database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}',
            'table_action': 'already_exists' if table_existed else 'created',
        }
    finally:
        client.disconnect()
