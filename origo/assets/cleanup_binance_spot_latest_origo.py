from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, asset

from .create_binance_spot_latest_tables_origo import (
    LATEST_DOLLAR_CUT_TABLES,
    LATEST_DOLLAR_KLINES_TABLE_NAME,
    LATEST_INGESTION_TABLE_NAME,
    LATEST_KLINES_TABLE_NAME,
    LATEST_RAW_TABLE_NAME,
    LATEST_TIME_CUT_TABLES,
    create_binance_spot_latest_tables_origo,
)
from .create_origo_database import get_clickhouse_settings, make_clickhouse_client
from .refresh_binance_spot_latest_cuts_origo import refresh_binance_spot_latest_cuts_origo

LATEST_RETENTION_DAYS = 2


@asset(
    deps=[create_binance_spot_latest_tables_origo, refresh_binance_spot_latest_cuts_origo],
    group_name='binance_data',
    description='Deletes rolling latest Binance spot rows beyond the two-day retention window.',
)
def cleanup_binance_spot_latest_origo(context: AssetExecutionContext) -> dict[str, object]:
    cutoff = datetime.now(UTC).replace(tzinfo=None, microsecond=0) - timedelta(
        days=LATEST_RETENTION_DAYS
    )
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        datetime_tables = (
            LATEST_RAW_TABLE_NAME,
            LATEST_KLINES_TABLE_NAME,
            *(table_name for _, table_name, _ in LATEST_TIME_CUT_TABLES),
        )
        start_datetime_tables = (
            LATEST_DOLLAR_KLINES_TABLE_NAME,
            *(table_name for _, table_name, _ in LATEST_DOLLAR_CUT_TABLES),
        )

        for table_name in datetime_tables:
            client.execute(
                f"""
                ALTER TABLE {settings.database}.{table_name}
                DELETE WHERE datetime < toDateTime('{cutoff:%Y-%m-%d %H:%M:%S}')
                """,
                settings={'mutations_sync': 2},
            )

        client.execute(
            f"""
            ALTER TABLE {settings.database}.{LATEST_INGESTION_TABLE_NAME}
            DELETE WHERE minute_start < toDateTime('{cutoff:%Y-%m-%d %H:%M:%S}')
            """,
            settings={'mutations_sync': 2},
        )

        for table_name in start_datetime_tables:
            client.execute(
                f"""
                ALTER TABLE {settings.database}.{table_name}
                DELETE WHERE start_datetime < toDateTime('{cutoff:%Y-%m-%d %H:%M:%S}')
                """,
                settings={'mutations_sync': 2},
            )

        return {
            'status': 'success',
            'cutoff': cutoff.strftime('%Y-%m-%dT%H:%M:%SZ'),
        }
    finally:
        client.disconnect()
