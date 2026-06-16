# How to add a new asset?

# 1. Add the asset to the assets folder
# 2. Add the asset to the imports below
# 3. Create a new job for the asset
# 4. Add the job to the assets list
# 5. Add the job to the jobs list
# 6. If applicable, add a schedule for the job and add it to the schedules list

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import (
    AssetKey,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunConfig,
    RunRequest,
    RunStatusSensorContext,
    RunStatusSensorDefinition,
    ScheduleDefinition,
    ScheduleEvaluationContext,
    SkipReason,
    TimeWindowPartitionsDefinition,
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
    DEPTH20_1M_TABLE_NAME,
    create_binance_spot_depth20_1m_table_origo,
)
from .assets.create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseClient as DepthClickHouseClient,
    SNAPSHOTS_TABLE_NAME as DEPTH20_SNAPSHOTS_TABLE_NAME,
    clickhouse_scalar_int as depth_clickhouse_scalar_int,
    create_binance_spot_depth20_snapshots_table_origo,
    get_clickhouse_settings as get_depth_clickhouse_settings,
    make_clickhouse_client as make_depth_clickhouse_client,
)
from .assets.create_binance_spot_depth200_1m_table_origo import (
    DEPTH200_1M_TABLE_NAME,
    create_binance_spot_depth200_1m_table_origo,
)
from .assets.create_binance_spot_depth200_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH200_SNAPSHOTS_TABLE_NAME,
    create_binance_spot_depth200_snapshots_table_origo,
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
from .assets.refresh_binance_spot_depth200_1m_origo import (
    refresh_binance_spot_depth200_1m_origo,
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
from .assets.sync_binance_spot_depth200_snapshots_to_origo import (
    depth200_minute_partitions,
    sync_binance_spot_depth200_snapshots_to_origo,
)
from .assets.reconcile_binance_spot_depth200_partition_state_origo import (
    reconcile_binance_spot_depth200_partition_state_origo,
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
from .assets.build_bar_store_arrow import (
    build_bar_store_arrow,
    bar_store_partition_run_requests,
    series_store_dir,
)
from .assets.build_depth_snapshot_store_arrow import (
    DepthSnapshotStoreConfig,
    LATEST_MANIFEST_NAME,
    build_depth_snapshot_store_arrow,
    depth_snapshot_chunk_relative_path,
    depth_snapshot_store_partition_run_request,
    minute_start_from_partition_key,
)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")
MAX_DAILY_BACKFILL_RUNS_PER_TICK = 14
MAX_AUTOMATED_DAILY_BACKFILL_GAP_DAYS = 14
DEPTH_SOURCE_LOOKBACK_MINUTES = 15


class _DagsterEventLike(Protocol):
    partition: str | None


class _AssetEventLike(Protocol):
    dagster_event: _DagsterEventLike | None


@dataclass(frozen=True)
class DepthLiveStoreStatus:
    snapshot_rows: int
    projection_rows: int
    arrow_chunk_exists: bool
    latest_manifest_minute: datetime | None


@dataclass(frozen=True)
class DepthLiveReconciliationSpec:
    label: str
    partitions: TimeWindowPartitionsDefinition
    run_key_prefix: str
    snapshot_table_name: str
    projection_table_name: str
    series: str
    base_url_env: str
    auth_token_env: str


DEPTH20_LIVE_RECONCILIATION_SPEC = DepthLiveReconciliationSpec(
    label='Depth20',
    partitions=depth20_minute_partitions,
    run_key_prefix='binance_spot_depth20',
    snapshot_table_name=DEPTH20_SNAPSHOTS_TABLE_NAME,
    projection_table_name=DEPTH20_1M_TABLE_NAME,
    series='depth20_snapshots',
    base_url_env='BINANCE_SPOT_DEPTH20_BASE_URL',
    auth_token_env='BINANCE_SPOT_DEPTH20_AUTH_TOKEN',
)

DEPTH200_LIVE_RECONCILIATION_SPEC = DepthLiveReconciliationSpec(
    label='Depth200',
    partitions=depth200_minute_partitions,
    run_key_prefix='binance_spot_depth200',
    snapshot_table_name=DEPTH200_SNAPSHOTS_TABLE_NAME,
    projection_table_name=DEPTH200_1M_TABLE_NAME,
    series='depth200_snapshots',
    base_url_env='BINANCE_SPOT_DEPTH200_BASE_URL',
    auth_token_env='BINANCE_SPOT_DEPTH200_AUTH_TOKEN',
)


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

create_binance_spot_depth200_snapshots_table_origo_job = define_asset_job(
    name='create_binance_spot_depth200_snapshots_table_origo_job',
    selection=['create_binance_spot_depth200_snapshots_table_origo'],
)

create_binance_spot_depth200_1m_table_origo_job = define_asset_job(
    name='create_binance_spot_depth200_1m_table_origo_job',
    selection=['create_binance_spot_depth200_1m_table_origo'],
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

_BINANCE_SPOT_DEPTH200_DATA_SOURCE_SELECTION = [
    'sync_binance_spot_depth200_snapshots_to_origo',
    'refresh_binance_spot_depth200_1m_origo',
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

refresh_binance_spot_depth200_data_source_job = define_asset_job(
    name='refresh_binance_spot_depth200_data_source_job',
    selection=_BINANCE_SPOT_DEPTH200_DATA_SOURCE_SELECTION,
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

backfill_binance_spot_depth200_data_source_job = define_asset_job(
    name='backfill_binance_spot_depth200_data_source_job',
    selection=_BINANCE_SPOT_DEPTH200_DATA_SOURCE_SELECTION,
)

repair_binance_spot_depth20_projection_job = define_asset_job(
    name='repair_binance_spot_depth20_projection_job',
    selection=['refresh_binance_spot_depth20_1m_origo'],
)

repair_binance_spot_depth200_projection_job = define_asset_job(
    name='repair_binance_spot_depth200_projection_job',
    selection=['refresh_binance_spot_depth200_1m_origo'],
)

reconcile_binance_spot_depth20_partition_state_origo_job = define_asset_job(
    name='reconcile_binance_spot_depth20_partition_state_origo_job',
    selection=['reconcile_binance_spot_depth20_partition_state_origo'],
)

reconcile_binance_spot_depth200_partition_state_origo_job = define_asset_job(
    name='reconcile_binance_spot_depth200_partition_state_origo_job',
    selection=['reconcile_binance_spot_depth200_partition_state_origo'],
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
    cron_schedule="* * * * *",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)

# Local Arrow Bar Store Jobs

build_bar_store_arrow_job = define_asset_job(
    name="build_bar_store_arrow_job",
    selection=[build_bar_store_arrow],
    executor_def=in_process_executor,
)

build_depth_snapshot_store_arrow_job = define_asset_job(
    name='build_depth_snapshot_store_arrow_job',
    selection=[build_depth_snapshot_store_arrow],
    executor_def=in_process_executor,
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


def _required_env_value(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'{name} environment variable must be set.')
    return value


def _depth_clickhouse_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def _depth_clickhouse_datetime64(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.000')


def _depth_source_history_url(base_url: str, minute_start: datetime) -> str:
    unix_seconds = int(minute_start.timestamp())
    return f'{base_url.rstrip("/")}/history?from={unix_seconds}&to={unix_seconds}'


def _depth_source_has_rows(spec: DepthLiveReconciliationSpec, minute_start: datetime) -> bool:
    try:
        response = requests.get(
            _depth_source_history_url(_required_env_value(spec.base_url_env), minute_start),
            headers={
                'Accept': 'application/x-ndjson',
                'Authorization': f'Bearer {_required_env_value(spec.auth_token_env)}',
            },
            timeout=30,
            stream=True,
        )
        try:
            response.raise_for_status()
            return any(line for line in response.iter_lines())
        finally:
            response.close()
    except requests.RequestException:
        return False


def _depth_snapshot_rows(
    client: DepthClickHouseClient,
    database: str,
    spec: DepthLiveReconciliationSpec,
    minute_start: datetime,
) -> int:
    minute_end = minute_start + timedelta(minutes=1)
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{spec.snapshot_table_name} FINAL
        WHERE datetime >= toDateTime64('{_depth_clickhouse_datetime64(minute_start)}', 3)
          AND datetime < toDateTime64('{_depth_clickhouse_datetime64(minute_end)}', 3)
        """
    )
    return depth_clickhouse_scalar_int(result)


def _depth_projection_rows(
    client: DepthClickHouseClient,
    database: str,
    spec: DepthLiveReconciliationSpec,
    minute_start: datetime,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{spec.projection_table_name} FINAL
        WHERE datetime = toDateTime('{_depth_clickhouse_datetime(minute_start)}')
        """
    )
    return depth_clickhouse_scalar_int(result)


def _depth_arrow_chunk_exists(spec: DepthLiveReconciliationSpec, minute_start: datetime) -> bool:
    chunk_path = series_store_dir(spec.series) / depth_snapshot_chunk_relative_path(minute_start)
    return chunk_path.is_file()


def _depth_latest_manifest_minute(spec: DepthLiveReconciliationSpec) -> datetime | None:
    manifest_path = series_store_dir(spec.series) / LATEST_MANIFEST_NAME
    if not manifest_path.exists():
        return None

    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    source_partition_key = manifest.get('source_partition_key')
    if not isinstance(source_partition_key, str):
        raise RuntimeError(f'{manifest_path} does not contain source_partition_key.')
    return minute_start_from_partition_key(source_partition_key)


def _depth_store_status(
    client: DepthClickHouseClient,
    database: str,
    spec: DepthLiveReconciliationSpec,
    minute_start: datetime,
) -> DepthLiveStoreStatus:
    return DepthLiveStoreStatus(
        snapshot_rows=_depth_snapshot_rows(client, database, spec, minute_start),
        projection_rows=_depth_projection_rows(client, database, spec, minute_start),
        arrow_chunk_exists=_depth_arrow_chunk_exists(spec, minute_start),
        latest_manifest_minute=_depth_latest_manifest_minute(spec),
    )


def _depth_arrow_is_complete(status: DepthLiveStoreStatus, minute_start: datetime) -> bool:
    return (
        status.arrow_chunk_exists
        and status.latest_manifest_minute is not None
        and status.latest_manifest_minute >= minute_start
    )


def _depth_candidate_minutes(
    context: ScheduleEvaluationContext,
    spec: DepthLiveReconciliationSpec,
) -> tuple[tuple[datetime, str], ...]:
    if DEPTH_SOURCE_LOOKBACK_MINUTES < 1:
        raise RuntimeError('DEPTH_SOURCE_LOOKBACK_MINUTES must be at least 1.')

    last_minute = _last_completed_minute(context.scheduled_execution_time)
    candidates: list[tuple[datetime, str]] = []
    for offset in reversed(range(DEPTH_SOURCE_LOOKBACK_MINUTES)):
        minute_start = last_minute - timedelta(minutes=offset)
        partition_key = spec.partitions.get_partition_key_for_timestamp(minute_start.timestamp())
        if spec.partitions.has_partition_key(partition_key):
            candidates.append((minute_start, partition_key))
    return tuple(candidates)


def _scheduled_run_key_suffix(context: ScheduleEvaluationContext) -> str:
    return _scheduled_time(context).astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')


def _depth_reconciliation_run_requests(
    context: ScheduleEvaluationContext,
    spec: DepthLiveReconciliationSpec,
) -> list[RunRequest] | SkipReason:
    settings = get_depth_clickhouse_settings()
    client = make_depth_clickhouse_client(settings)
    run_key_suffix = _scheduled_run_key_suffix(context)
    run_requests: list[RunRequest] = []

    try:
        for minute_start, partition_key in _depth_candidate_minutes(context, spec):
            status = _depth_store_status(client, settings.database, spec, minute_start)
            if status.snapshot_rows == 0 and _depth_source_has_rows(spec, minute_start):
                run_requests.append(
                    RunRequest(
                        partition_key=partition_key,
                        run_key=f'{spec.run_key_prefix}:source:{partition_key}:{run_key_suffix}',
                    )
                )
    finally:
        client.disconnect()

    if not run_requests:
        return SkipReason(f'{spec.label}: no source-available raw depth gaps in lookback.')
    return run_requests


def _depth_projection_reconciliation_run_requests(
    context: ScheduleEvaluationContext,
    spec: DepthLiveReconciliationSpec,
) -> list[RunRequest] | SkipReason:
    settings = get_depth_clickhouse_settings()
    client = make_depth_clickhouse_client(settings)
    run_key_suffix = _scheduled_run_key_suffix(context)
    run_requests: list[RunRequest] = []

    try:
        for minute_start, partition_key in _depth_candidate_minutes(context, spec):
            status = _depth_store_status(client, settings.database, spec, minute_start)
            if status.snapshot_rows > 0 and status.projection_rows == 0:
                run_requests.append(
                    RunRequest(
                        partition_key=partition_key,
                        run_key=f'{spec.run_key_prefix}:projection:{partition_key}:{run_key_suffix}',
                    )
                )
    finally:
        client.disconnect()

    if not run_requests:
        return SkipReason(f'{spec.label}: no raw-present projection gaps in lookback.')
    return run_requests


def _depth_arrow_reconciliation_run_requests(
    context: ScheduleEvaluationContext,
    spec: DepthLiveReconciliationSpec,
) -> list[RunRequest] | SkipReason:
    settings = get_depth_clickhouse_settings()
    client = make_depth_clickhouse_client(settings)
    run_key_suffix = _scheduled_run_key_suffix(context)
    run_requests: list[RunRequest] = []

    try:
        for minute_start, partition_key in _depth_candidate_minutes(context, spec):
            status = _depth_store_status(client, settings.database, spec, minute_start)
            if status.snapshot_rows > 0 and not _depth_arrow_is_complete(status, minute_start):
                run_requests.append(
                    RunRequest(
                        partition_key=spec.series,
                        run_key=f'{spec.run_key_prefix}:arrow:{partition_key}:{run_key_suffix}',
                        run_config=RunConfig(
                            ops={
                                'build_depth_snapshot_store_arrow': DepthSnapshotStoreConfig(
                                    source_partition_key=partition_key
                                )
                            }
                        ),
                    )
                )
    finally:
        client.disconnect()

    if not run_requests:
        return SkipReason(f'{spec.label}: no raw-present Arrow gaps in lookback.')
    return run_requests


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
def binance_spot_depth20_1m_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _depth_reconciliation_run_requests(context, DEPTH20_LIVE_RECONCILIATION_SPEC)


@schedule(
    job=repair_binance_spot_depth20_projection_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_depth20_projection_repair_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _depth_projection_reconciliation_run_requests(
        context, DEPTH20_LIVE_RECONCILIATION_SPEC
    )


@schedule(
    job=build_depth_snapshot_store_arrow_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_depth20_arrow_repair_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _depth_arrow_reconciliation_run_requests(context, DEPTH20_LIVE_RECONCILIATION_SPEC)


@schedule(
    job=refresh_binance_spot_depth200_data_source_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_depth200_1m_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _depth_reconciliation_run_requests(context, DEPTH200_LIVE_RECONCILIATION_SPEC)


@schedule(
    job=repair_binance_spot_depth200_projection_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_depth200_projection_repair_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _depth_projection_reconciliation_run_requests(
        context, DEPTH200_LIVE_RECONCILIATION_SPEC
    )


@schedule(
    job=build_depth_snapshot_store_arrow_job,
    cron_schedule='* * * * *',
    execution_timezone='UTC',
    default_status=DefaultScheduleStatus.RUNNING,
)
def binance_spot_depth200_arrow_repair_schedule(
    context: ScheduleEvaluationContext,
) -> list[RunRequest] | SkipReason:
    return _depth_arrow_reconciliation_run_requests(context, DEPTH200_LIVE_RECONCILIATION_SPEC)


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


def _bar_store_on_mirror_success(context: RunStatusSensorContext) -> list[RunRequest]:
    """When the Parquet mirror job succeeds, rebuild every Arrow series.

    Event-driven off the mirror job's completion (not a clock or a file poll):
    each successful mirror tick fans out one Arrow run per series, run-keyed to
    the mirror run so the same success is never acted on twice. The asset's
    content-hash versioning still suppresses a republish when bytes are unchanged.
    """
    return bar_store_partition_run_requests(context.dagster_run.run_id)


def _depth_snapshot_store_on_source_success(context: RunStatusSensorContext) -> RunRequest:
    source_partition_key = context.dagster_run.tags.get('dagster/partition')
    if source_partition_key is None:
        raise RuntimeError('Depth snapshot source run must have a dagster/partition tag.')
    return depth_snapshot_store_partition_run_request(
        context.dagster_run.job_name,
        context.dagster_run.run_id,
        source_partition_key,
    )


# Built as a RunStatusSensorDefinition (class) rather than the @run_status_sensor
# decorator: the decorator factory is partially typed in the dagster stubs (would add a
# pyright error), while the class constructor types cleanly.
bar_store_source_sensor = RunStatusSensorDefinition(
    name="bar_store_source_sensor",
    run_status=DagsterRunStatus.SUCCESS,
    run_status_sensor_fn=_bar_store_on_mirror_success,
    monitored_jobs=[publish_binance_spot_klines_to_mount_job],
    request_job=build_bar_store_arrow_job,
    default_status=DefaultSensorStatus.RUNNING,
)

depth_snapshot_store_source_sensor = RunStatusSensorDefinition(
    name='depth_snapshot_store_source_sensor',
    run_status=DagsterRunStatus.SUCCESS,
    run_status_sensor_fn=_depth_snapshot_store_on_source_success,
    monitored_jobs=[
        refresh_binance_spot_depth20_data_source_job,
        refresh_binance_spot_depth200_data_source_job,
    ],
    request_job=build_depth_snapshot_store_arrow_job,
    default_status=DefaultSensorStatus.RUNNING,
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
            create_binance_spot_depth200_snapshots_table_origo,
            create_binance_spot_depth200_1m_table_origo,
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
            sync_binance_spot_depth200_snapshots_to_origo,
            refresh_binance_spot_depth200_1m_origo,
            reconcile_binance_spot_depth200_partition_state_origo,
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
            build_bar_store_arrow,
            build_depth_snapshot_store_arrow,
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
        binance_spot_depth20_projection_repair_schedule,
        binance_spot_depth20_arrow_repair_schedule,
        binance_spot_depth200_1m_schedule,
        binance_spot_depth200_projection_repair_schedule,
        binance_spot_depth200_arrow_repair_schedule,
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
        bar_store_source_sensor,
        depth_snapshot_store_source_sensor,
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
          create_binance_spot_depth200_snapshots_table_origo_job,
          create_binance_spot_depth200_1m_table_origo_job,
          create_binance_spot_latest_tables_origo_job,
          create_aligned_1m_exchange_table_origo_job,
          create_binance_trades_complete_view_job,
          insert_monthly_binance_trades_job,
          refresh_binance_spot_data_source_job,
          backfill_binance_spot_dollar_klines_origo_job,
          backfill_binance_spot_trades_origo_job,
          refresh_binance_futures_data_source_job,
          refresh_binance_spot_depth20_data_source_job,
          refresh_binance_spot_depth200_data_source_job,
          refresh_binance_spot_latest_data_source_job,
          backfill_binance_spot_depth20_data_source_job,
          backfill_binance_spot_depth200_data_source_job,
          repair_binance_spot_depth20_projection_job,
          repair_binance_spot_depth200_projection_job,
          reconcile_binance_spot_depth20_partition_state_origo_job,
          reconcile_binance_spot_depth200_partition_state_origo_job,
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
          build_bar_store_arrow_job,
          build_depth_snapshot_store_arrow_job,
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
