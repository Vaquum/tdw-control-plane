# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

import os
from datetime import datetime, timedelta, timezone
from typing import Protocol

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import (
    AssetKey,
    DefaultScheduleStatus,
    Definitions,
    RunConfig,
    RunRequest,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    asset_sensor,
    build_schedule_from_partitioned_job,
    define_asset_job,
    in_process_executor,
    schedule,
)

from .assets.cleanup_binance_daily_trades import (
    cleanup_binance_daily_trades_for_finalized_month,
)
from .assets.daily_trades_to_origo import insert_daily_binance_spot_trades_to_origo
from .assets.daily_trades_to_tdw import insert_daily_binance_trades_to_tdw
from .assets.publish_binance_spot_klines_to_huggingface import (
    publish_binance_spot_klines_to_huggingface,
)
from .assets.publish_binance_spot_15m_klines_to_huggingface import (
    publish_binance_spot_15m_klines_to_huggingface,
)
from .assets.publish_binance_spot_30m_klines_to_huggingface import (
    publish_binance_spot_30m_klines_to_huggingface,
)
from .assets.publish_binance_spot_1h_klines_to_huggingface import (
    publish_binance_spot_1h_klines_to_huggingface,
)
from .assets.publish_binance_spot_2h_klines_to_huggingface import (
    publish_binance_spot_2h_klines_to_huggingface,
)
from .assets.publish_binance_spot_4h_klines_to_huggingface import (
    publish_binance_spot_4h_klines_to_huggingface,
)
from .assets.publish_binance_spot_1M_dollar_klines_to_huggingface import (
    publish_binance_spot_1M_dollar_klines_to_huggingface,
)
from .assets.publish_binance_spot_15M_dollar_klines_to_huggingface import (
    publish_binance_spot_15M_dollar_klines_to_huggingface,
)
from .assets.publish_binance_spot_30M_dollar_klines_to_huggingface import (
    publish_binance_spot_30M_dollar_klines_to_huggingface,
)
from .assets.publish_binance_spot_60M_dollar_klines_to_huggingface import (
    publish_binance_spot_60M_dollar_klines_to_huggingface,
)
from .assets.publish_binance_spot_120M_dollar_klines_to_huggingface import (
    publish_binance_spot_120M_dollar_klines_to_huggingface,
)
from .assets.publish_binance_spot_240M_dollar_klines_to_huggingface import (
    publish_binance_spot_240M_dollar_klines_to_huggingface,
)
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
from .assets.create_binance_trades_table_origo import (
    create_binance_daily_spot_trades_table_origo,
)
from .assets.create_binance_futures_trades_table_origo import (
    create_binance_daily_futures_trades_table_origo,
)
from .assets.create_binance_spot_klines_table_origo import (
    create_binance_spot_klines_table_origo,
)
from .assets.create_binance_spot_dollar_klines_table_origo import (
    create_binance_spot_dollar_klines_table_origo,
)
from .assets.create_binance_spot_volume_klines_table_origo import (
    create_binance_spot_volume_klines_table_origo,
)
from .assets.create_binance_spot_tick_klines_table_origo import (
    create_binance_spot_tick_klines_table_origo,
)
from .assets.create_binance_spot_dollar_imbalance_klines_table_origo import (
    create_binance_spot_dollar_imbalance_klines_table_origo,
)
from .assets.create_binance_futures_klines_table_origo import (
    create_binance_futures_klines_table_origo,
)
from .assets.create_binance_spot_depth20_1m_table_origo import (
    create_binance_spot_depth20_1m_table_origo,
)
from .assets.create_binance_spot_depth20_snapshots_table_origo import (
    create_binance_spot_depth20_snapshots_table_origo,
)
from .assets.refresh_binance_spot_klines_origo import refresh_binance_spot_klines_origo
from .assets.refresh_binance_spot_dollar_klines_origo import (
    refresh_binance_spot_dollar_klines_origo,
)
from .assets.refresh_binance_spot_volume_klines_origo import (
    refresh_binance_spot_volume_klines_origo,
)
from .assets.refresh_binance_spot_tick_klines_origo import (
    refresh_binance_spot_tick_klines_origo,
)
from .assets.refresh_binance_spot_dollar_imbalance_klines_origo import (
    refresh_binance_spot_dollar_imbalance_klines_origo,
)
from .assets.refresh_binance_futures_klines_origo import refresh_binance_futures_klines_origo
from .assets.refresh_binance_spot_depth20_1m_origo import (
    refresh_binance_spot_depth20_1m_origo,
)
from .assets.create_aligned_1m_exchange_table_origo import (
    create_aligned_1m_exchange_table_origo,
)
from .assets.daily_futures_trades_to_origo import insert_daily_binance_futures_trades_to_origo
from .assets.refresh_aligned_1m_exchange_from_binance_spot_origo import (
    refresh_aligned_1m_exchange_from_binance_spot_origo,
)
from .assets.refresh_aligned_1m_exchange_from_binance_futures_origo import (
    refresh_aligned_1m_exchange_from_binance_futures_origo,
)
from .assets.sync_binance_spot_depth20_snapshots_to_origo import (
    depth20_minute_partitions,
    sync_binance_spot_depth20_snapshots_to_origo,
)
from .assets.reconcile_binance_spot_depth20_partition_state_origo import (
    reconcile_binance_spot_depth20_partition_state_origo,
)
from .assets.create_binance_spot_latest_tables_origo import (
    create_binance_spot_latest_tables_origo,
)
from .assets.sync_binance_spot_trades_latest_origo import (
    LATEST_MINUTE_START_TAG,
    sync_binance_spot_trades_latest_origo,
)
from .assets.refresh_binance_spot_klines_latest_origo import (
    refresh_binance_spot_klines_latest_origo,
)
from .assets.refresh_binance_spot_dollar_klines_latest_origo import (
    refresh_binance_spot_dollar_klines_latest_origo,
)
from .assets.refresh_binance_spot_latest_cuts_origo import (
    refresh_binance_spot_latest_cuts_origo,
)
from .assets.cleanup_binance_spot_latest_origo import cleanup_binance_spot_latest_origo
from .assets.publish_binance_spot_klines_to_mount import (
    MOUNT_EXPORT_ASSETS,
    SPECS as MOUNT_EXPORT_SPECS,
    MountExportConfig,
)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")
MAX_DAILY_BACKFILL_RUNS_PER_TICK = 14
MAX_AUTOMATED_DAILY_BACKFILL_GAP_DAYS = 14


class _DagsterEventLike(Protocol):
    partition: str | None


class _AssetEventLike(Protocol):
    dagster_event: _DagsterEventLike | None


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

create_binance_daily_spot_trades_table_origo_job = define_asset_job(
    name="create_binance_daily_spot_trades_table_origo_job",
    selection=["create_binance_daily_spot_trades_table_origo"]
)

create_binance_daily_futures_trades_table_origo_job = define_asset_job(
    name="create_binance_daily_futures_trades_table_origo_job",
    selection=["create_binance_daily_futures_trades_table_origo"]
)

create_binance_spot_klines_table_origo_job = define_asset_job(
    name="create_binance_spot_klines_table_origo_job",
    selection=["create_binance_spot_klines_table_origo"]
)

create_binance_spot_dollar_klines_table_origo_job = define_asset_job(
    name="create_binance_spot_dollar_klines_table_origo_job",
    selection=["create_binance_spot_dollar_klines_table_origo"]
)

create_binance_spot_volume_klines_table_origo_job = define_asset_job(
    name="create_binance_spot_volume_klines_table_origo_job",
    selection=["create_binance_spot_volume_klines_table_origo"]
)

create_binance_spot_tick_klines_table_origo_job = define_asset_job(
    name="create_binance_spot_tick_klines_table_origo_job",
    selection=["create_binance_spot_tick_klines_table_origo"]
)

create_binance_spot_dollar_imbalance_klines_table_origo_job = define_asset_job(
    name="create_binance_spot_dollar_imbalance_klines_table_origo_job",
    selection=["create_binance_spot_dollar_imbalance_klines_table_origo"]
)

create_binance_futures_klines_table_origo_job = define_asset_job(
    name="create_binance_futures_klines_table_origo_job",
    selection=["create_binance_futures_klines_table_origo"]
)

create_binance_spot_depth20_snapshots_table_origo_job = define_asset_job(
    name="create_binance_spot_depth20_snapshots_table_origo_job",
    selection=["create_binance_spot_depth20_snapshots_table_origo"]
)

create_binance_spot_depth20_1m_table_origo_job = define_asset_job(
    name="create_binance_spot_depth20_1m_table_origo_job",
    selection=["create_binance_spot_depth20_1m_table_origo"]
)

create_binance_spot_latest_tables_origo_job = define_asset_job(
    name='create_binance_spot_latest_tables_origo_job',
    selection=['create_binance_spot_latest_tables_origo'],
)

create_aligned_1m_exchange_table_origo_job = define_asset_job(
    name="create_aligned_1m_exchange_table_origo_job",
    selection=["create_aligned_1m_exchange_table_origo"]
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

refresh_binance_spot_data_source_job = define_asset_job(
    name="refresh_binance_spot_data_source_job",
    selection=[
        "insert_daily_binance_spot_trades_to_origo",
        "refresh_binance_spot_klines_origo",
        "refresh_binance_spot_dollar_klines_origo",
        "refresh_binance_spot_volume_klines_origo",
        "refresh_binance_spot_tick_klines_origo",
        "refresh_binance_spot_dollar_imbalance_klines_origo",
        "refresh_aligned_1m_exchange_from_binance_spot_origo",
    ])

backfill_binance_spot_dollar_klines_origo_job = define_asset_job(
    name='backfill_binance_spot_dollar_klines_origo_job',
    selection=['refresh_binance_spot_dollar_klines_origo'],
)

backfill_binance_spot_trades_origo_job = define_asset_job(
    name='backfill_binance_spot_trades_origo_job',
    selection=['insert_daily_binance_spot_trades_to_origo'],
)

_BINANCE_SPOT_DEPTH20_DATA_SOURCE_SELECTION = [
    'sync_binance_spot_depth20_snapshots_to_origo',
    'refresh_binance_spot_depth20_1m_origo',
]

refresh_binance_futures_data_source_job = define_asset_job(
    name="refresh_binance_futures_data_source_job",
    selection=[
        "insert_daily_binance_futures_trades_to_origo",
        "refresh_binance_futures_klines_origo",
        "refresh_aligned_1m_exchange_from_binance_futures_origo",
    ])

refresh_binance_spot_depth20_data_source_job = define_asset_job(
    name='refresh_binance_spot_depth20_data_source_job',
    selection=_BINANCE_SPOT_DEPTH20_DATA_SOURCE_SELECTION,
)

refresh_binance_spot_latest_data_source_job = define_asset_job(
    name='refresh_binance_spot_latest_data_source_job',
    selection=[
        'create_binance_spot_latest_tables_origo',
        'sync_binance_spot_trades_latest_origo',
        'refresh_binance_spot_klines_latest_origo',
        'refresh_binance_spot_dollar_klines_latest_origo',
        'refresh_binance_spot_latest_cuts_origo',
        'cleanup_binance_spot_latest_origo',
    ],
)

backfill_binance_spot_depth20_data_source_job = define_asset_job(
    name='backfill_binance_spot_depth20_data_source_job',
    selection=_BINANCE_SPOT_DEPTH20_DATA_SOURCE_SELECTION,
)

reconcile_binance_spot_depth20_partition_state_origo_job = define_asset_job(
    name='reconcile_binance_spot_depth20_partition_state_origo_job',
    selection=['reconcile_binance_spot_depth20_partition_state_origo'],
)

insert_daily_binance_trades_tdw_job = define_asset_job(
    name="insert_daily_trades_to_tdw_job",
    selection=["insert_daily_binance_trades_to_tdw"])

publish_binance_spot_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_klines_to_huggingface_job",
    selection=["publish_binance_spot_klines_to_huggingface"])

publish_binance_spot_15m_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_15m_klines_to_huggingface_job",
    selection=["publish_binance_spot_15m_klines_to_huggingface"])

publish_binance_spot_30m_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_30m_klines_to_huggingface_job",
    selection=["publish_binance_spot_30m_klines_to_huggingface"])

publish_binance_spot_1h_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_1h_klines_to_huggingface_job",
    selection=["publish_binance_spot_1h_klines_to_huggingface"])

publish_binance_spot_2h_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_2h_klines_to_huggingface_job",
    selection=["publish_binance_spot_2h_klines_to_huggingface"])

publish_binance_spot_4h_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_4h_klines_to_huggingface_job",
    selection=["publish_binance_spot_4h_klines_to_huggingface"])

publish_binance_spot_1M_dollar_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_1M_dollar_klines_to_huggingface_job",
    selection=["publish_binance_spot_1M_dollar_klines_to_huggingface"])

publish_binance_spot_15M_dollar_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_15M_dollar_klines_to_huggingface_job",
    selection=["publish_binance_spot_15M_dollar_klines_to_huggingface"])

publish_binance_spot_30M_dollar_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_30M_dollar_klines_to_huggingface_job",
    selection=["publish_binance_spot_30M_dollar_klines_to_huggingface"])

publish_binance_spot_60M_dollar_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_60M_dollar_klines_to_huggingface_job",
    selection=["publish_binance_spot_60M_dollar_klines_to_huggingface"])

publish_binance_spot_120M_dollar_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_120M_dollar_klines_to_huggingface_job",
    selection=["publish_binance_spot_120M_dollar_klines_to_huggingface"])

publish_binance_spot_240M_dollar_klines_to_huggingface_job = define_asset_job(
    name="publish_binance_spot_240M_dollar_klines_to_huggingface_job",
    selection=["publish_binance_spot_240M_dollar_klines_to_huggingface"])

# Local Parquet Mirror Jobs

publish_binance_spot_klines_to_mount_job = define_asset_job(
    name="publish_binance_spot_klines_to_mount_job",
    selection=MOUNT_EXPORT_ASSETS,
    executor_def=in_process_executor,
)

backfill_binance_spot_klines_to_mount_job = define_asset_job(
    name="backfill_binance_spot_klines_to_mount_job",
    selection=MOUNT_EXPORT_ASSETS,
    executor_def=in_process_executor,
    config=RunConfig(
        ops={
            f"export_{spec.name}_to_mount": MountExportConfig(mode="backfill")
            for spec in MOUNT_EXPORT_SPECS
        }
    ),
)

publish_binance_spot_klines_to_mount_schedule = ScheduleDefinition(
    name="publish_binance_spot_klines_to_mount_schedule",
    job=publish_binance_spot_klines_to_mount_job,
    cron_schedule="*/10 * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

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
        password=_get_clickhouse_password(),
        database=CLICKHOUSE_DATABASE,
    )


def _get_clickhouse_password():
    if not CLICKHOUSE_PASSWORD:
        raise RuntimeError(
            "CLICKHOUSE_PASSWORD environment variable must be set before using ClickHouse-dependent schedules."
        )

    return CLICKHOUSE_PASSWORD


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


def _latest_day_in_table(table_name: str):
    client = None
    try:
        client = _get_clickhouse_client()
        result = client.execute(
            f"""
            SELECT max(toDate(datetime))
            FROM {CLICKHOUSE_DATABASE}.{table_name}
        """
        )
        return result[0][0]
    finally:
        if client:
            client.disconnect()


def _existing_days_in_range(table_name: str, start_date: str, end_date: str) -> set:
    client = None
    try:
        client = _get_clickhouse_client()
        result = client.execute(
            f"""
            SELECT DISTINCT toDate(datetime) AS day
            FROM {CLICKHOUSE_DATABASE}.{table_name}
            WHERE toDate(datetime) >= toDate('{start_date}')
              AND toDate(datetime) <= toDate('{end_date}')
        """
        )
        return {row[0] for row in result if row[0] is not None}
    finally:
        if client:
            client.disconnect()


def _next_daily_overlay_start_day():
    latest_monthly_day = _latest_day_in_table("binance_trades")
    if latest_monthly_day is None:
        return None

    return latest_monthly_day + timedelta(days=1)


def _binance_archive_available(file_url: str) -> bool:
    checksum_url = f"{file_url}.CHECKSUM"
    try:
        response = requests.get(checksum_url, timeout=30)
        return response.status_code == 200
    except requests.RequestException:
        return False


def _scheduled_time(context: ScheduleEvaluationContext) -> datetime:
    return context.scheduled_execution_time or datetime.now(timezone.utc)


def _last_completed_minute(scheduled_time: datetime | None) -> datetime:
    reference_time = scheduled_time or datetime.now(timezone.utc)
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    return reference_time.astimezone(timezone.utc).replace(second=0, microsecond=0) - timedelta(
        minutes=1
    )


daily_binance_spot_pipeline_schedule = build_schedule_from_partitioned_job(
    refresh_binance_spot_data_source_job,
    name='daily_binance_spot_pipeline_schedule',
    hour_of_day=4,
    default_status=DefaultScheduleStatus.RUNNING,
)


daily_binance_futures_pipeline_schedule = build_schedule_from_partitioned_job(
    refresh_binance_futures_data_source_job,
    name='daily_binance_futures_pipeline_schedule',
    hour_of_day=10,
    default_status=DefaultScheduleStatus.RUNNING,
)


@schedule(
    job=refresh_binance_spot_depth20_data_source_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_depth20_1m_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    minute_start = _last_completed_minute(context.scheduled_execution_time)
    partition_key = depth20_minute_partitions.get_partition_key_for_timestamp(
        minute_start.timestamp()
    )
    if not depth20_minute_partitions.has_partition_key(partition_key):
        return SkipReason(f'Depth20 partition {partition_key} is before the partition start.')

    return RunRequest(
        partition_key=partition_key,
        run_key=f'binance_spot_depth20::{partition_key}',
    )


@schedule(
    job=refresh_binance_spot_latest_data_source_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_latest_1m_schedule(context: ScheduleEvaluationContext) -> RunRequest | SkipReason:
    minute_start = _last_completed_minute(context.scheduled_execution_time)
    minute_key = minute_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    return RunRequest(
        run_key=f'binance_spot_latest::{minute_key}',
        tags={LATEST_MINUTE_START_TAG: minute_key},
    )


@schedule(
    job=insert_daily_binance_trades_tdw_job,
    cron_schedule="0 */4 * * *",
    execution_timezone="UTC",
)
def daily_tdw_pipeline_schedule(context: ScheduleEvaluationContext):
    scheduled_time = _scheduled_time(context)
    end_date = (scheduled_time - timedelta(days=1)).date()

    if not _table_exists("binance_daily_trades"):
        return SkipReason("tdw.binance_daily_trades does not exist yet.")

    if not _table_exists("binance_trades"):
        return SkipReason("tdw.binance_trades does not exist yet.")

    start_day = _next_daily_overlay_start_day()
    if start_day is None or start_day > end_date:
        return SkipReason("No missing daily overlay partitions need to be materialized.")

    backfill_gap_days = (end_date - start_day).days + 1
    if backfill_gap_days > MAX_AUTOMATED_DAILY_BACKFILL_GAP_DAYS:
        return SkipReason(
            "Daily overlay gap is larger than the automated backfill threshold; trigger a manual backfill."
        )

    existing_days = _existing_days_in_range(
        "binance_daily_trades",
        start_day.isoformat(),
        end_date.isoformat(),
    )

    run_requests = []
    current_day = start_day
    while current_day <= end_date and len(run_requests) < MAX_DAILY_BACKFILL_RUNS_PER_TICK:
        if current_day not in existing_days:
            target_date = current_day.isoformat()
            file_url = (
                "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
                f"BTCUSDT-trades-{target_date}.zip"
            )
            if _binance_archive_available(file_url):
                run_requests.append(
                    RunRequest(
                        partition_key=target_date,
                        run_key=f"binance_daily_trades::{target_date}",
                    )
                )
        current_day += timedelta(days=1)

    if not run_requests:
        return SkipReason("No available Binance daily archives were found for missing overlay days.")

    return run_requests


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
        return SkipReason("tdw.binance_trades does not exist yet.")

    if _count_rows_for_month("binance_trades", previous_month_start) > 0:
        return SkipReason(f"Monthly Binance trades for {previous_month_start} are already loaded.")

    file_url = (
        "https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/"
        f"BTCUSDT-trades-{previous_month_label}.zip"
    )
    if not _binance_archive_available(file_url):
        return SkipReason(f"Monthly Binance archive {previous_month_label} is not available yet.")

    return RunRequest(
        partition_key=previous_month_start,
        run_key=f"binance_trades::{previous_month_start}",
    )


def _publish_binance_spot_klines_to_hf_run_request(
    asset_event: _AssetEventLike,
    *,
    run_key_prefix: str,
) -> RunRequest | SkipReason:
    if not asset_event.dagster_event:
        return SkipReason(
            "No Dagster event was attached to the Origo spot trades materialization."
        )

    partition_key = asset_event.dagster_event.partition
    if partition_key is None:
        return SkipReason("Origo spot trades materialization did not include a partition key.")

    return RunRequest(
        partition_key=partition_key,
        run_key=f"{run_key_prefix}::{partition_key}",
    )


def _publish_binance_spot_dollar_klines_to_hf_run_request(
    asset_event: _AssetEventLike,
    *,
    run_key_prefix: str,
) -> RunRequest | SkipReason:
    if not asset_event.dagster_event:
        return SkipReason(
            "No Dagster event was attached to the Origo dollar klines materialization."
        )

    partition_key = asset_event.dagster_event.partition
    if partition_key is None:
        return SkipReason("Origo dollar klines materialization did not include a partition key.")

    return RunRequest(
        partition_key=partition_key,
        run_key=f"{run_key_prefix}::{partition_key}",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_klines_origo"),
    job=publish_binance_spot_klines_to_huggingface_job,
)
def publish_binance_spot_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_klines_origo"),
    job=publish_binance_spot_15m_klines_to_huggingface_job,
)
def publish_binance_spot_15m_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_15m_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_klines_origo"),
    job=publish_binance_spot_30m_klines_to_huggingface_job,
)
def publish_binance_spot_30m_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_30m_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_klines_origo"),
    job=publish_binance_spot_1h_klines_to_huggingface_job,
)
def publish_binance_spot_1h_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_1h_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_klines_origo"),
    job=publish_binance_spot_2h_klines_to_huggingface_job,
)
def publish_binance_spot_2h_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_2h_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_klines_origo"),
    job=publish_binance_spot_4h_klines_to_huggingface_job,
)
def publish_binance_spot_4h_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_4h_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_dollar_klines_origo"),
    job=publish_binance_spot_1M_dollar_klines_to_huggingface_job,
)
def publish_binance_spot_1M_dollar_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_dollar_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_1M_dollar_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_dollar_klines_origo"),
    job=publish_binance_spot_15M_dollar_klines_to_huggingface_job,
)
def publish_binance_spot_15M_dollar_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_dollar_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_15M_dollar_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_dollar_klines_origo"),
    job=publish_binance_spot_30M_dollar_klines_to_huggingface_job,
)
def publish_binance_spot_30M_dollar_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_dollar_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_30M_dollar_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_dollar_klines_origo"),
    job=publish_binance_spot_60M_dollar_klines_to_huggingface_job,
)
def publish_binance_spot_60M_dollar_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_dollar_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_60M_dollar_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_dollar_klines_origo"),
    job=publish_binance_spot_120M_dollar_klines_to_huggingface_job,
)
def publish_binance_spot_120M_dollar_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_dollar_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_120M_dollar_klines_to_hf",
    )


@asset_sensor(
    asset_key=AssetKey("refresh_binance_spot_dollar_klines_origo"),
    job=publish_binance_spot_240M_dollar_klines_to_huggingface_job,
)
def publish_binance_spot_240M_dollar_klines_to_huggingface_sensor(
    context: object,
    asset_event: _AssetEventLike,
) -> RunRequest | SkipReason:
    return _publish_binance_spot_dollar_klines_to_hf_run_request(
        asset_event,
        run_key_prefix="publish_binance_spot_240M_dollar_klines_to_hf",
    )


defs = Definitions(
    assets=[create_tdw_database,
            create_origo_database,
            create_binance_trades_table,
            create_binance_daily_trades_table,
            create_binance_daily_spot_trades_table_origo,
            create_binance_daily_futures_trades_table_origo,
            create_binance_spot_klines_table_origo,
            create_binance_spot_dollar_klines_table_origo,
            create_binance_spot_volume_klines_table_origo,
            create_binance_spot_tick_klines_table_origo,
            create_binance_spot_dollar_imbalance_klines_table_origo,
            create_binance_futures_klines_table_origo,
            create_binance_spot_depth20_snapshots_table_origo,
            create_binance_spot_depth20_1m_table_origo,
            create_binance_spot_latest_tables_origo,
            create_aligned_1m_exchange_table_origo,
            create_binance_trades_complete_view,
            insert_monthly_binance_trades_to_tdw,
            insert_daily_binance_spot_trades_to_origo,
            insert_daily_binance_futures_trades_to_origo,
            refresh_binance_spot_klines_origo,
            refresh_binance_spot_dollar_klines_origo,
            refresh_binance_spot_volume_klines_origo,
            refresh_binance_spot_tick_klines_origo,
            refresh_binance_spot_dollar_imbalance_klines_origo,
            refresh_binance_futures_klines_origo,
            sync_binance_spot_depth20_snapshots_to_origo,
            refresh_binance_spot_depth20_1m_origo,
            reconcile_binance_spot_depth20_partition_state_origo,
            sync_binance_spot_trades_latest_origo,
            refresh_binance_spot_klines_latest_origo,
            refresh_binance_spot_dollar_klines_latest_origo,
            refresh_binance_spot_latest_cuts_origo,
            cleanup_binance_spot_latest_origo,
            refresh_aligned_1m_exchange_from_binance_spot_origo,
            refresh_aligned_1m_exchange_from_binance_futures_origo,
            insert_daily_binance_trades_to_tdw,
            publish_binance_spot_klines_to_huggingface,
            publish_binance_spot_15m_klines_to_huggingface,
            publish_binance_spot_30m_klines_to_huggingface,
            publish_binance_spot_1h_klines_to_huggingface,
            publish_binance_spot_2h_klines_to_huggingface,
            publish_binance_spot_4h_klines_to_huggingface,
            publish_binance_spot_1M_dollar_klines_to_huggingface,
            publish_binance_spot_15M_dollar_klines_to_huggingface,
            publish_binance_spot_30M_dollar_klines_to_huggingface,
            publish_binance_spot_60M_dollar_klines_to_huggingface,
            publish_binance_spot_120M_dollar_klines_to_huggingface,
            publish_binance_spot_240M_dollar_klines_to_huggingface,
            *MOUNT_EXPORT_ASSETS,
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
        daily_binance_spot_pipeline_schedule,
        daily_binance_futures_pipeline_schedule,
        binance_spot_depth20_1m_schedule,
        binance_spot_latest_1m_schedule,
        daily_tdw_pipeline_schedule,
        monthly_tdw_rollforward_schedule,
        publish_binance_spot_klines_to_mount_schedule,
    ],

    sensors=[
        publish_binance_spot_klines_to_huggingface_sensor,
        publish_binance_spot_15m_klines_to_huggingface_sensor,
        publish_binance_spot_30m_klines_to_huggingface_sensor,
        publish_binance_spot_1h_klines_to_huggingface_sensor,
        publish_binance_spot_2h_klines_to_huggingface_sensor,
        publish_binance_spot_4h_klines_to_huggingface_sensor,
        publish_binance_spot_1M_dollar_klines_to_huggingface_sensor,
        publish_binance_spot_15M_dollar_klines_to_huggingface_sensor,
        publish_binance_spot_30M_dollar_klines_to_huggingface_sensor,
        publish_binance_spot_60M_dollar_klines_to_huggingface_sensor,
        publish_binance_spot_120M_dollar_klines_to_huggingface_sensor,
        publish_binance_spot_240M_dollar_klines_to_huggingface_sensor,
    ],
    
    jobs=[create_tdw_database_job,
          create_origo_database_job,
          create_binance_trades_table_job,
          create_binance_daily_trades_table_job,
          create_binance_daily_spot_trades_table_origo_job,
          create_binance_daily_futures_trades_table_origo_job,
          create_binance_spot_klines_table_origo_job,
          create_binance_spot_dollar_klines_table_origo_job,
          create_binance_spot_volume_klines_table_origo_job,
          create_binance_spot_tick_klines_table_origo_job,
          create_binance_spot_dollar_imbalance_klines_table_origo_job,
          create_binance_futures_klines_table_origo_job,
          create_binance_spot_depth20_snapshots_table_origo_job,
          create_binance_spot_depth20_1m_table_origo_job,
          create_binance_spot_latest_tables_origo_job,
          create_aligned_1m_exchange_table_origo_job,
          create_binance_trades_complete_view_job,
          insert_monthly_binance_trades_job,
          refresh_binance_spot_data_source_job,
          backfill_binance_spot_dollar_klines_origo_job,
          backfill_binance_spot_trades_origo_job,
          refresh_binance_futures_data_source_job,
          refresh_binance_spot_depth20_data_source_job,
          refresh_binance_spot_latest_data_source_job,
          backfill_binance_spot_depth20_data_source_job,
          reconcile_binance_spot_depth20_partition_state_origo_job,
          insert_daily_binance_trades_tdw_job,
          publish_binance_spot_klines_to_huggingface_job,
          publish_binance_spot_15m_klines_to_huggingface_job,
          publish_binance_spot_30m_klines_to_huggingface_job,
          publish_binance_spot_1h_klines_to_huggingface_job,
          publish_binance_spot_2h_klines_to_huggingface_job,
          publish_binance_spot_4h_klines_to_huggingface_job,
          publish_binance_spot_1M_dollar_klines_to_huggingface_job,
          publish_binance_spot_15M_dollar_klines_to_huggingface_job,
          publish_binance_spot_30M_dollar_klines_to_huggingface_job,
          publish_binance_spot_60M_dollar_klines_to_huggingface_job,
          publish_binance_spot_120M_dollar_klines_to_huggingface_job,
          publish_binance_spot_240M_dollar_klines_to_huggingface_job,
          publish_binance_spot_klines_to_mount_job,
          backfill_binance_spot_klines_to_mount_job,
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
