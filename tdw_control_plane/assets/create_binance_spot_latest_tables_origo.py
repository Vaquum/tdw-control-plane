from collections.abc import Iterable

from dagster import AssetExecutionContext, asset

from .create_binance_trades_table_origo import database_exists, table_exists
from .create_origo_database import (
    ClickHouseSettings,
    create_origo_database,
    get_clickhouse_settings,
    make_clickhouse_client,
)

LATEST_RAW_TABLE_NAME = 'binance_spot_trades_latest'
LATEST_INGESTION_TABLE_NAME = 'binance_spot_trades_latest_ingestion'
LATEST_WATERMARKS_TABLE_NAME = 'binance_spot_latest_watermarks'
LATEST_KLINES_TABLE_NAME = 'binance_spot_klines_latest'
LATEST_DOLLAR_KLINES_TABLE_NAME = 'binance_spot_dollar_klines_latest'
LATEST_TIME_CUT_TABLES = (
    ('15m', 'binance_spot_15m_klines_latest', 15),
    ('30m', 'binance_spot_30m_klines_latest', 30),
    ('1h', 'binance_spot_1h_klines_latest', 60),
    ('2h', 'binance_spot_2h_klines_latest', 120),
    ('4h', 'binance_spot_4h_klines_latest', 240),
)
LATEST_DOLLAR_CUT_TABLES = (
    ('15M', 'binance_spot_15M_dollar_klines_latest', 15),
    ('30M', 'binance_spot_30M_dollar_klines_latest', 30),
    ('60M', 'binance_spot_60M_dollar_klines_latest', 60),
    ('120M', 'binance_spot_120M_dollar_klines_latest', 120),
    ('240M', 'binance_spot_240M_dollar_klines_latest', 240),
)


def _latest_raw_table_sql(settings: ClickHouseSettings) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{LATEST_RAW_TABLE_NAME} (
            minute_start DateTime,
            trade_id UInt64,
            price Float64,
            quantity Float64,
            quote_quantity Float64,
            timestamp UInt64,
            is_buyer_maker UInt8,
            is_best_match UInt8,
            datetime DateTime64(3)
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(minute_start)
        ORDER BY (minute_start, trade_id)
        TTL datetime + INTERVAL 2 DAY DELETE
        """


def _latest_ingestion_table_sql(settings: ClickHouseSettings) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{LATEST_INGESTION_TABLE_NAME} (
            minute_start DateTime,
            start_trade_id UInt64,
            end_trade_id UInt64,
            row_count UInt64,
            status LowCardinality(String),
            dagster_run_id String,
            loaded_at DateTime
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(minute_start)
        ORDER BY (minute_start, status)
        TTL minute_start + INTERVAL 2 DAY DELETE
        """


def _latest_watermarks_table_sql(settings: ClickHouseSettings) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{LATEST_WATERMARKS_TABLE_NAME} (
            layer LowCardinality(String),
            watermark_minute DateTime,
            updated_at DateTime
        )
        ENGINE = MergeTree()
        ORDER BY layer
        """


def _kline_table_sql(settings: ClickHouseSettings, table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{table_name} (
            datetime DateTime,
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
        PARTITION BY toYYYYMMDD(datetime)
        ORDER BY datetime
        TTL datetime + INTERVAL 2 DAY DELETE
        """


def _dollar_kline_table_sql(settings: ClickHouseSettings, table_name: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{table_name} (
            start_datetime DateTime,
            end_datetime DateTime,
            dollar_bar_id UInt64,
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
        PARTITION BY toYYYYMMDD(start_datetime)
        ORDER BY (start_datetime, end_datetime, dollar_bar_id)
        TTL start_datetime + INTERVAL 2 DAY DELETE
        """


def latest_table_names() -> tuple[str, ...]:
    return (
        LATEST_RAW_TABLE_NAME,
        LATEST_INGESTION_TABLE_NAME,
        LATEST_WATERMARKS_TABLE_NAME,
        LATEST_KLINES_TABLE_NAME,
        LATEST_DOLLAR_KLINES_TABLE_NAME,
        *(table_name for _, table_name, _ in LATEST_TIME_CUT_TABLES),
        *(table_name for _, table_name, _ in LATEST_DOLLAR_CUT_TABLES),
    )


def _latest_table_sql(settings: ClickHouseSettings) -> Iterable[str]:
    yield _latest_raw_table_sql(settings)
    yield _latest_ingestion_table_sql(settings)
    yield _latest_watermarks_table_sql(settings)
    yield _kline_table_sql(settings, LATEST_KLINES_TABLE_NAME)
    yield _dollar_kline_table_sql(settings, LATEST_DOLLAR_KLINES_TABLE_NAME)
    for _, table_name, _ in LATEST_TIME_CUT_TABLES:
        yield _kline_table_sql(settings, table_name)
    for _, table_name, _ in LATEST_DOLLAR_CUT_TABLES:
        yield _dollar_kline_table_sql(settings, table_name)


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description='Creates rolling latest Binance spot raw, watermark, kline, and cut tables.',
)
def create_binance_spot_latest_tables_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        if not database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        table_actions: dict[str, str] = {}
        for table_name in latest_table_names():
            table_actions[table_name] = (
                'already_exists' if table_exists(client, settings, table_name) else 'created'
            )

        for sql in _latest_table_sql(settings):
            client.execute(sql)

        context.log.info(f'Ensured {len(table_actions)} rolling latest Binance spot tables exist.')
        return {
            'status': 'success',
            'tables': table_actions,
        }
    finally:
        client.disconnect()
