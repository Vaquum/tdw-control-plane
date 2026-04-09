import os
from clickhouse_driver import Client as ClickhouseClient
from dagster import asset, AssetExecutionContext

from tdw_control_plane.assets.monthly_trades_to_tdw import (
    insert_monthly_binance_trades_to_tdw,
    monthly_partitions,
)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")


def _get_clickhouse_password():
    if not CLICKHOUSE_PASSWORD:
        raise RuntimeError(
            "CLICKHOUSE_PASSWORD environment variable must be set before creating the ClickHouse client."
        )

    return CLICKHOUSE_PASSWORD


@asset(
    partitions_def=monthly_partitions,
    deps=[insert_monthly_binance_trades_to_tdw],
    group_name="binance_data",
    description="Deletes finalized months from tdw.binance_daily_trades once the monthly archive has landed in tdw.binance_trades",
)
def cleanup_binance_daily_trades_for_finalized_month(context: AssetExecutionContext):
    partition_date_str = context.asset_partition_key_for_output()
    date_parts = partition_date_str.split("-")
    year, month = date_parts[0], date_parts[1]
    month_start = f"{year}-{month}-01"

    client = None
    try:
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=_get_clickhouse_password(),
            database=CLICKHOUSE_DATABASE,
            compression=True,
            send_receive_timeout=900,
        )

        monthly_count = client.execute(
            f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.binance_trades
            WHERE datetime >= toDate('{month_start}')
              AND datetime < addMonths(toDate('{month_start}'), 1)
        """
        )[0][0]

        if monthly_count == 0:
            context.log.info(
                f"No finalized monthly data found for {month_start}. Skipping daily cleanup."
            )
            return {
                "month_start": month_start,
                "rows_deleted": 0,
                "status": "skipped",
            }

        daily_count = client.execute(
            f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.binance_daily_trades
            WHERE datetime >= toDate('{month_start}')
              AND datetime < addMonths(toDate('{month_start}'), 1)
        """
        )[0][0]

        if daily_count == 0:
            context.log.info(
                f"No overlay rows found for {month_start}. Nothing to delete from binance_daily_trades."
            )
            return {
                "month_start": month_start,
                "rows_deleted": 0,
                "status": "no_overlay_rows",
            }

        client.execute(
            f"""
            ALTER TABLE {CLICKHOUSE_DATABASE}.binance_daily_trades
            DELETE WHERE datetime >= toDate('{month_start}')
              AND datetime < addMonths(toDate('{month_start}'), 1)
        """
        )
        context.log.info(
            f"Queued deletion of {daily_count} overlay rows for finalized month {month_start}."
        )

        return {
            "month_start": month_start,
            "rows_deleted": daily_count,
            "status": "cleanup_queued",
        }

    except Exception:
        raise
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass
