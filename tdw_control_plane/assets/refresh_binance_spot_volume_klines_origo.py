import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Protocol, runtime_checkable

from dagster import AssetExecutionContext, asset

from .create_binance_spot_volume_klines_table_origo import (
    VOLUME_KLINES_TABLE_NAME,
    create_binance_spot_volume_klines_table_origo,
)
from .create_binance_trades_table_origo import RAW_TABLE_NAME
from .daily_trades_to_origo import daily_partitions, insert_daily_binance_spot_trades_to_origo

_DEFAULT_CLICKHOUSE_HOST = 'clickhouse'
_DEFAULT_CLICKHOUSE_PORT = 9000
_DEFAULT_CLICKHOUSE_USER = 'default'
VOLUME_KLINE_SIZE = 100.0


@dataclass(frozen=True)
class _ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@runtime_checkable
class _ClickHouseClientLike(Protocol):
    def execute(
        self,
        query: str,
        params: object | None = None,
        *,
        settings: Mapping[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'{name} environment variable must be set.')
    return value


def _get_clickhouse_port() -> int:
    value = os.environ.get('CLICKHOUSE_PORT', str(_DEFAULT_CLICKHOUSE_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_PORT environment variable must be an integer.') from exc


def _get_clickhouse_settings() -> _ClickHouseSettings:
    return _ClickHouseSettings(
        host=os.environ.get('CLICKHOUSE_HOST', _DEFAULT_CLICKHOUSE_HOST),
        port=_get_clickhouse_port(),
        user=os.environ.get('CLICKHOUSE_USER', _DEFAULT_CLICKHOUSE_USER),
        password=_require_env('CLICKHOUSE_PASSWORD'),
        database=os.environ.get('CLICKHOUSE_DATABASE', 'origo'),
    )


def _make_clickhouse_client(settings: _ClickHouseSettings) -> _ClickHouseClientLike:
    client_factory = getattr(import_module('clickhouse_driver'), 'Client')
    client = client_factory(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )
    if not isinstance(client, _ClickHouseClientLike):
        raise TypeError('clickhouse_driver.Client does not satisfy the ClickHouse client contract.')
    return client


def _partition_date_from_context(context: AssetExecutionContext) -> str:
    partition_key = context.partition_key
    if isinstance(partition_key, str):
        return partition_key

    target_date = datetime.now(UTC) - timedelta(days=1)
    return target_date.strftime('%Y-%m-%d')


def _delete_partition_rows(
    client: _ClickHouseClientLike,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{VOLUME_KLINES_TABLE_NAME}
        DELETE WHERE toDate(start_datetime) = toDate('{partition_date}')
        """,
        settings={'mutations_sync': 2},
    )


def _count_partition_rows(
    client: _ClickHouseClientLike,
    database: str,
    partition_date: str,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{VOLUME_KLINES_TABLE_NAME}
        WHERE toDate(start_datetime) = toDate('{partition_date}')
        """
    )
    count_value = result[0][0]
    if not isinstance(count_value, int):
        raise RuntimeError(f'ClickHouse count query returned non-integer value: {count_value!r}')
    return count_value


def _insert_partition_rows(
    client: _ClickHouseClientLike,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO {database}.{VOLUME_KLINES_TABLE_NAME}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            volume_bar_id,
            argMin(price, trade_id) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, trade_id) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
            sumKahan(quantity) AS volume,
            avg(is_buyer_maker) AS maker_ratio,
            count() AS no_of_trades,
            argMin(price * quantity, trade_id) AS open_liquidity,
            max(price * quantity) AS high_liquidity,
            min(price * quantity) AS low_liquidity,
            argMax(price * quantity, trade_id) AS close_liquidity,
            sum(price * quantity) AS liquidity_sum,
            sumKahan(is_buyer_maker * quantity) AS maker_volume,
            sum(is_buyer_maker * price * quantity) AS maker_liquidity
        FROM (
            SELECT
                *,
                toUInt64(floor(running_volume_before / {VOLUME_KLINE_SIZE})) AS volume_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum(quantity) OVER (
                            ORDER BY datetime, trade_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - quantity,
                        0.0
                    ) AS running_volume_before
                FROM {database}.{RAW_TABLE_NAME}
                WHERE toDate(datetime) = toDate('{partition_date}')
            )
        )
        GROUP BY volume_bar_id
        ORDER BY volume_bar_id
        """
    )


@asset(
    partitions_def=daily_partitions,
    deps=[
        create_binance_spot_volume_klines_table_origo,
        insert_daily_binance_spot_trades_to_origo,
    ],
    group_name='binance_data',
    description=(
        'Refreshes the daily-scoped Binance spot volume kline projection from '
        'source-native daily trades; volume_bar_id resets each date and the final '
        'daily bar may be below the volume threshold.'
    ),
)
def refresh_binance_spot_volume_klines_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date = _partition_date_from_context(context)
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing Binance spot volume kline rows for '
                f'{partition_date}. Replacing that partition.'
            )
            _delete_partition_rows(client, settings.database, partition_date)

        _insert_partition_rows(client, settings.database, partition_date)
        inserted_count = _count_partition_rows(client, settings.database, partition_date)

        return {
            'status': 'success',
            'partition_date': partition_date,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{VOLUME_KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
