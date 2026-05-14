import os
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Protocol, cast

from dagster import AssetExecutionContext, asset

from .create_origo_database import create_origo_database

DEFAULT_CLICKHOUSE_HOST = 'clickhouse'
DEFAULT_CLICKHOUSE_PORT = 9000
DEFAULT_CLICKHOUSE_USER = 'default'

SNAPSHOTS_TABLE_NAME = 'binance_spot_depth20_snapshots'


class ClickHouseClient(Protocol):
    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: object | None = None,
    ) -> object:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'{name} environment variable must be set.')
    return value


def _get_clickhouse_port() -> int:
    value = os.environ.get('CLICKHOUSE_PORT', str(DEFAULT_CLICKHOUSE_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_PORT environment variable must be an integer.') from exc


def get_clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        host=os.environ.get('CLICKHOUSE_HOST', DEFAULT_CLICKHOUSE_HOST),
        port=_get_clickhouse_port(),
        user=os.environ.get('CLICKHOUSE_USER', DEFAULT_CLICKHOUSE_USER),
        password=_require_env('CLICKHOUSE_PASSWORD'),
        database=os.environ.get('CLICKHOUSE_DATABASE', 'origo'),
    )


def make_clickhouse_client(settings: ClickHouseSettings) -> ClickHouseClient:
    client_factory = getattr(import_module('clickhouse_driver'), 'Client')
    if not callable(client_factory):
        raise TypeError('clickhouse_driver.Client is not callable.')

    return cast(
        ClickHouseClient,
        client_factory(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
        ),
    )


def clickhouse_scalar_int(result: object) -> int:
    if not isinstance(result, Sequence) or isinstance(result, (bytes, str)) or not result:
        raise TypeError('Expected non-empty ClickHouse row list.')

    rows = cast(Sequence[object], result)
    row = rows[0]
    if not isinstance(row, Sequence) or isinstance(row, (bytes, str)) or not row:
        raise TypeError('Expected ClickHouse tuple row.')

    values = cast(Sequence[object], row)
    value = values[0]
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f'Expected ClickHouse int scalar, got {type(value).__name__}.')

    return value


def _create_snapshots_table(client: ClickHouseClient, settings: ClickHouseSettings) -> None:
    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.database}.{SNAPSHOTS_TABLE_NAME} (
            datetime DateTime64(3),
            source_timestamp_ms UInt64,
            last_update_id UInt64,
            bids Array(Tuple(Float64, Float64)),
            asks Array(Tuple(Float64, Float64))
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(datetime)
        ORDER BY datetime
        """
    )


@asset(
    group_name='origo_setup',
    deps=[create_origo_database],
    description='Creates the binance_spot_depth20_snapshots table if it does not exist',
)
def create_binance_spot_depth20_snapshots_table_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        _create_snapshots_table(client, settings)

        context.log.info(f'Ensured table {settings.database}.{SNAPSHOTS_TABLE_NAME} exists.')
        return {
            'status': 'success',
            'table': f'{settings.database}.{SNAPSHOTS_TABLE_NAME}',
        }
    finally:
        client.disconnect()
