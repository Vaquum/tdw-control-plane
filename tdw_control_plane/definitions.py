# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

import os
from datetime import datetime, timedelta, timezone

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import (
    Definitions,
    RunRequest,
    ScheduleEvaluationContext,
    define_asset_job,
    schedule,
)

from .assets.cleanup_binance_daily_trades import (
    cleanup_binance_daily_trades_for_finalized_month,
)
from .assets.daily_trades_to_origo import insert_daily_binance_trades_to_origo
from .assets.daily_trades_to_tdw import insert_daily_binance_trades_to_tdw
from .assets.monthly_trades_to_tdw import insert_monthly_binance_trades_to_tdw
from .assets.create_tdw_database import create_tdw_database
from .assets.create_binance_daily_trades_table import create_binance_daily_trades_table
from .assets.create_binance_trades_table import create_binance_trades_table
from .assets.create_binance_trades_complete_view import (
    create_binance_trades_complete_view,
)
from .assets.create_binance_trades_monthly_summary import create_binance_trades_monthly_summary
from .assets.create_binance_trades_daily_summary import create_binance_trades_daily_summary
from .assets.create_binance_trades_hourly_summary import create_binance_trades_hourly_summary
from .assets.create_binance_trades_hour_of_day_summary import create_binance_trades_hour_of_day_summary
from .assets.create_binance_trades_day_of_month_summary import create_binance_trades_day_of_month_summary
from .assets.create_binance_trades_week_of_year_summary import create_binance_trades_week_of_year_summary
from .assets.create_binance_trades_month_of_year_summary import create_binance_trades_month_of_year_summary
from .assets.create_binance_agg_trades_table import create_binance_agg_trades_table
from .assets.monthly_agg_trades_to_tdw import insert_monthly_binance_agg_trades_to_tdw
from .assets.create_binance_futures_trades_table import create_binance_futures_trades_table
from .assets.monthly_futures_trades_to_tdw import insert_monthly_binance_futures_trades_to_tdw
from .assets.monthly_futures_agg_trades_to_tdw import create_binance_futures_agg_trades_table
from .assets.monthly_futures_agg_trades_to_tdw import insert_monthly_binance_futures_agg_trades_to_tdw
from .assets.create_origo_database import create_origo_database
from .assets.create_binance_trades_table_origo import create_binance_trades_table_origo

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")


# Database Maintenance Jobs

create_tdw_database_job = define_asset_job(
    name="create_tdw_database_job",
    selection=["create_tdw_database"])

create_origo_database_job = define_asset_job(
    name="create_origo_database_job",
    selection=["create_origo_database"]
)

create_binance_trades_table_job = define_asset_job(
    name="create_binance_trades_table_job",
    selection=["create_binance_trades_table"])

create_binance_daily_trades_table_job = define_asset_job(
    name="create_binance_daily_trades_table_job",
    selection=["create_binance_daily_trades_table"])

create_binance_trades_table_origo_job = define_asset_job(
    name="create_binance_trades_table_origo_job",
    selection=["create_binance_trades_table_origo"]
)

create_binance_trades_complete_view_job = define_asset_job(
    name="create_binance_trades_complete_view_job",
    selection=["create_binance_trades_complete_view"])

create_binance_agg_trades_table_job = define_asset_job(
    name="create_binance_agg_trades_table_job",
    selection=["create_binance_agg_trades_table"])

create_binance_futures_trades_table_job = define_asset_job(
    name="create_binance_futures_trades_table_job",
    selection=["create_binance_futures_trades_table"])

create_binance_futures_agg_trades_table_job = define_asset_job(
    name="create_binance_futures_agg_trades_table_job",
    selection=["create_binance_futures_agg_trades_table"])

# Data Insertion Jobs

insert_monthly_binance_trades_job = define_asset_job(
    name="insert_monthly_trades_to_tdw_job",
    selection=["insert_monthly_binance_trades_to_tdw"])

insert_daily_binance_trades_job = define_asset_job(
    name="insert_daily_trades_to_origo_job",
    selection=["insert_daily_binance_trades_to_origo"])

insert_daily_binance_trades_tdw_job = define_asset_job(
    name="insert_daily_trades_to_tdw_job",
    selection=["insert_daily_binance_trades_to_tdw"])

insert_monthly_binance_agg_trades_job = define_asset_job(
    name="insert_monthly_agg_trades_to_tdw_job",
    selection=["insert_monthly_binance_agg_trades_to_tdw"])

roll_forward_monthly_binance_trades_job = define_asset_job(
    name="roll_forward_monthly_binance_trades_job",
    selection=[
        "insert_monthly_binance_trades_to_tdw",
        "cleanup_binance_daily_trades_for_finalized_month",
    ])

insert_monthly_binance_futures_trades_job = define_asset_job(
    name="insert_monthly_futures_trades_to_tdw_job",
    selection=["insert_monthly_binance_futures_trades_to_tdw"])

insert_monthly_binance_futures_agg_trades_job = define_asset_job(
    name="insert_monthly_futures_agg_trades_to_tdw_job",
    selection=["insert_monthly_binance_futures_agg_trades_to_tdw"])

# summary Table Creation Jobs

create_binance_trades_monthly_summary_job = define_asset_job(
    name="create_binance_trades_monthly_summary_job",
    selection=["create_binance_trades_monthly_summary"])

create_binance_trades_daily_summary_job = define_asset_job(
    name="create_binance_trades_daily_summary_job",
    selection=["create_binance_trades_daily_summary"])

create_binance_trades_hourly_summary_job = define_asset_job(
    name="create_binance_trades_hourly_summary_job",
    selection=["create_binance_trades_hourly_summary"])

create_binance_trades_hour_of_day_summary_job = define_asset_job(
    name="create_binance_trades_hour_of_day_summary_job",
    selection=["create_binance_trades_hour_of_day_summary"])

create_binance_trades_day_of_month_summary_job = define_asset_job(
    name="create_binance_trades_day_of_month_summary_job",
    selection=["create_binance_trades_day_of_month_summary"])

create_binance_trades_week_of_year_summary_job = define_asset_job(
    name="create_binance_trades_week_of_year_summary_job",
    selection=["create_binance_trades_week_of_year_summary"])

create_binance_trades_month_of_year_summary_job = define_asset_job(
    name="create_binance_trades_month_of_year_summary_job",
    selection=["create_binance_trades_month_of_year_summary"])


def _get_clickhouse_client():
    return ClickhouseClient(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def _table_exists(table_name: str) -> bool:
    client = None
    try:
        client = _get_clickhouse_client()
        result = client.execute(
            f"""
            SELECT count(*)
            FROM system.tables
            WHERE database = '{CLICKHOUSE_DATABASE}'
              AND name = '{table_name}'
        """
        )
        return bool(result[0][0])
    finally:
        if client:
            client.disconnect()


def _count_rows_for_day(table_name: str, date_str: str) -> int:
    if not _table_exists(table_name):
        return 0

    client = None
    try:
        client = _get_clickhouse_client()
        result = client.execute(
            f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{table_name}
            WHERE toDate(datetime) = toDate('{date_str}')
        """
        )
        return result[0][0]
    finally:
        if client:
            client.disconnect()


def _count_rows_for_month(table_name: str, month_start: str) -> int:
    if not _table_exists(table_name):
        return 0

    client = None
    try:
        client = _get_clickhouse_client()
        result = client.execute(
            f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{table_name}
            WHERE datetime >= toDate('{month_start}')
              AND datetime < addMonths(toDate('{month_start}'), 1)
        """
        )
        return result[0][0]
    finally:
        if client:
            client.disconnect()


def _binance_archive_available(file_url: str) -> bool:
    checksum_url = f"{file_url}.CHECKSUM"
    try:
        response = requests.get(checksum_url, timeout=30)
        return response.status_code == 200
    except requests.RequestException:
        return False


def _scheduled_time(context: ScheduleEvaluationContext) -> datetime:
    return context.scheduled_execution_time or datetime.now(timezone.utc)


@schedule(
    job=insert_daily_binance_trades_job,
    cron_schedule="0 1 * * *",
    execution_timezone="UTC")

def daily_pipeline_schedule():
    return {}


@schedule(
    job=insert_daily_binance_trades_tdw_job,
    cron_schedule="0 */4 * * *",
    execution_timezone="UTC",
)
def daily_tdw_pipeline_schedule(context: ScheduleEvaluationContext):
    scheduled_time = _scheduled_time(context)
    target_date = (scheduled_time - timedelta(days=1)).date().isoformat()

    if not _table_exists("binance_daily_trades"):
        return None

    if _count_rows_for_day("binance_daily_trades", target_date) > 0:
        return None

    file_url = (
        "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
        f"BTCUSDT-trades-{target_date}.zip"
    )
    if not _binance_archive_available(file_url):
        return None

    return RunRequest(partition_key=target_date)


@schedule(
    job=roll_forward_monthly_binance_trades_job,
    cron_schedule="0 9 * * *",
    execution_timezone="UTC",
)
def monthly_tdw_rollforward_schedule(context: ScheduleEvaluationContext):
    scheduled_time = _scheduled_time(context)
    current_month_start = scheduled_time.date().replace(day=1)
    previous_month_date = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_date.replace(day=1).isoformat()
    previous_month_label = previous_month_date.strftime("%Y-%m")

    if not _table_exists("binance_trades"):
        return None

    if _count_rows_for_month("binance_trades", previous_month_start) > 0:
        return None

    file_url = (
        "https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/"
        f"BTCUSDT-trades-{previous_month_label}.zip"
    )
    if not _binance_archive_available(file_url):
        return None

    return RunRequest(partition_key=previous_month_start)

defs = Definitions(
    assets=[create_tdw_database,
            create_origo_database,
            create_binance_trades_table,
            create_binance_daily_trades_table,
            create_binance_trades_table_origo,
            create_binance_trades_complete_view,
            insert_monthly_binance_trades_to_tdw,
            insert_daily_binance_trades_to_origo,
            insert_daily_binance_trades_to_tdw,
            cleanup_binance_daily_trades_for_finalized_month,
            create_binance_trades_monthly_summary,
            create_binance_trades_daily_summary,
            create_binance_trades_hourly_summary,
            create_binance_trades_hour_of_day_summary,
            create_binance_trades_day_of_month_summary,
            create_binance_trades_week_of_year_summary,
            create_binance_trades_month_of_year_summary,
            create_binance_agg_trades_table,
            insert_monthly_binance_agg_trades_to_tdw,
            create_binance_futures_trades_table,
            insert_monthly_binance_futures_trades_to_tdw,
            create_binance_futures_agg_trades_table,
            insert_monthly_binance_futures_agg_trades_to_tdw],
    
    schedules=[
        daily_pipeline_schedule,
        daily_tdw_pipeline_schedule,
        monthly_tdw_rollforward_schedule,
    ],
    
    jobs=[create_tdw_database_job,
          create_origo_database_job,
          create_binance_trades_table_job,
          create_binance_daily_trades_table_job,
          create_binance_trades_table_origo_job,
          create_binance_trades_complete_view_job,
          insert_monthly_binance_trades_job,
          insert_daily_binance_trades_job,
          insert_daily_binance_trades_tdw_job,
          roll_forward_monthly_binance_trades_job,
          create_binance_trades_monthly_summary_job,
          create_binance_trades_daily_summary_job,
          create_binance_trades_hourly_summary_job,
          create_binance_trades_hour_of_day_summary_job,
          create_binance_trades_day_of_month_summary_job,
          create_binance_trades_week_of_year_summary_job,
          create_binance_trades_month_of_year_summary_job,
          create_binance_agg_trades_table_job,
          insert_monthly_binance_agg_trades_job,
          create_binance_futures_trades_table_job,
          insert_monthly_binance_futures_trades_job,
          create_binance_futures_agg_trades_table_job,
          insert_monthly_binance_futures_agg_trades_job])

# TODO: Put everything in to same order in all segments of the code
