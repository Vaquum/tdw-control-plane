from collections.abc import Sequence
from datetime import datetime, timezone
from typing import cast

from dagster import AssetExecutionContext, AssetKey, AssetMaterialization, asset

from .create_binance_spot_depth20_1m_table_origo import DEPTH20_1M_TABLE_NAME
from .create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseClient,
    SNAPSHOTS_TABLE_NAME,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .refresh_binance_spot_depth20_1m_origo import refresh_binance_spot_depth20_1m_origo
from .sync_binance_spot_depth20_snapshots_to_origo import (
    DEPTH20_PARTITION_KEY_FORMAT,
    sync_binance_spot_depth20_snapshots_to_origo,
)


def _datetime_rows(result: object) -> list[datetime]:
    if not isinstance(result, Sequence) or isinstance(result, (bytes, str)):
        raise TypeError('Expected ClickHouse row list.')

    rows = cast(Sequence[object], result)
    values: list[datetime] = []
    for row in rows:
        if (
            not isinstance(row, Sequence)
            or isinstance(row, (bytes, str))
            or len(row) != 1
        ):
            raise TypeError('Expected ClickHouse single-value row.')
        value = row[0]
        if not isinstance(value, datetime):
            raise TypeError(
                f'Expected ClickHouse datetime value, got {type(value).__name__}.'
            )
        values.append(value)
    return values


def _partition_key(minute: datetime) -> str:
    if minute.tzinfo is None:
        minute = minute.replace(tzinfo=timezone.utc)
    return minute.astimezone(timezone.utc).strftime(DEPTH20_PARTITION_KEY_FORMAT)


def _existing_snapshot_minutes(client: ClickHouseClient, database: str) -> list[datetime]:
    return _datetime_rows(
        client.execute(
            f"""
            SELECT DISTINCT toStartOfMinute(datetime) AS minute
            FROM {database}.{SNAPSHOTS_TABLE_NAME}
            ORDER BY minute
            """
        )
    )


def _existing_projection_minutes(client: ClickHouseClient, database: str) -> list[datetime]:
    return _datetime_rows(
        client.execute(
            f"""
            SELECT DISTINCT datetime AS minute
            FROM {database}.{DEPTH20_1M_TABLE_NAME}
            ORDER BY minute
            """
        )
    )


def _report_existing_partitions(
    context: AssetExecutionContext,
    *,
    asset_key: AssetKey,
    database: str,
    table_name: str,
    minutes: list[datetime],
) -> int:
    existing_partitions = context.instance.get_materialized_partitions(asset_key)
    reported_count = 0

    for minute in minutes:
        partition_key = _partition_key(minute)
        if partition_key in existing_partitions:
            continue

        context.instance.report_runless_asset_event(
            AssetMaterialization(
                asset_key=asset_key,
                partition=partition_key,
                metadata={'source_table': f'{database}.{table_name}'},
            )
        )
        reported_count += 1

    return reported_count


@asset(
    group_name='binance_spot_depth20_data',
    description='Reconciles Dagster depth20 partition state from existing Origo ClickHouse rows.',
)
def reconcile_binance_spot_depth20_partition_state_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        snapshot_minutes = _existing_snapshot_minutes(client, settings.database)
        projection_minutes = _existing_projection_minutes(client, settings.database)

        reported_snapshot_count = _report_existing_partitions(
            context,
            asset_key=sync_binance_spot_depth20_snapshots_to_origo.key,
            database=settings.database,
            table_name=SNAPSHOTS_TABLE_NAME,
            minutes=snapshot_minutes,
        )
        reported_projection_count = _report_existing_partitions(
            context,
            asset_key=refresh_binance_spot_depth20_1m_origo.key,
            database=settings.database,
            table_name=DEPTH20_1M_TABLE_NAME,
            minutes=projection_minutes,
        )

        return {
            'snapshot_minutes_found': len(snapshot_minutes),
            'projection_minutes_found': len(projection_minutes),
            'snapshot_partitions_reported': reported_snapshot_count,
            'projection_partitions_reported': reported_projection_count,
        }
    finally:
        client.disconnect()
