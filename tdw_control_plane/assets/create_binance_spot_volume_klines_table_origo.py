import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Protocol, runtime_checkable

from dagster import AssetExecutionContext, asset

from .create_origo_database import create_origo_database

_DEFAULT_CLICKHOUSE_HOST = 'clickhouse'
_DEFAULT_CLICKHOUSE_PORT = 9000
_DEFAULT_CLICKHOUSE_USER = 'default'
VOLUME_KLINES_TABLE_NAME = 'binance_spot_volume_klines'


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


def _count_result(result: list[tuple[object, ...]]) -> int:
    count_value = result[0][0]
    if not isinstance(count_value, int):
        raise RuntimeError(f'ClickHouse count query returned non-integer value: {count_value!r}')
    return count_value


def _database_exists(client: _ClickHouseClientLike, database: str) -> bool:
    result = client.execute(
        f"""
        SELECT count()
        FROM system.databases
        WHERE name = '{database}'
        """
    )
    return bool(_count_result(result))


def _table_exists(
    client: _ClickHouseClientLike,
    settings: _ClickHouseSettings,
    table_name: str,
) -> bool:
    result = client.execute(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{settings.database}'
          AND name = '{table_name}'
        """
    )
    return bool(_count_result(result))


def _create_volume_klines_table(
    client: _ClickHouseClientLike,
    settings: _ClickHouseSettings,
) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{VOLUME_KLINES_TABLE_NAME} (
            start_datetime DateTime,
            end_datetime DateTime,
            volume_bar_id UInt64 COMMENT 'Partition-date scoped id; resets each day and may skip ids when one trade crosses multiple volume thresholds.',
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
        ORDER BY (start_datetime, end_datetime, volume_bar_id)
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description=(
        'Creates the binance_spot_volume_klines table if it does not exist. '
        'volume_bar_id is scoped to each partition date and may be non-contiguous.'
    ),
)
def create_binance_spot_volume_klines_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        if not _database_exists(client, settings.database):
            raise RuntimeError(
                f'Database {settings.database} does not exist. Run create_origo_database first.'
            )

        table_existed = _table_exists(client, settings, VOLUME_KLINES_TABLE_NAME)
        _create_volume_klines_table(client, settings)

        context.log.info(f'Ensured table {settings.database}.{VOLUME_KLINES_TABLE_NAME} exists.')
        return {
            'status': 'success',
            'table': f'{settings.database}.{VOLUME_KLINES_TABLE_NAME}',
            'table_action': 'already_exists' if table_existed else 'created',
        }
    finally:
        client.disconnect()
