import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset

__all__ = [
    'ClickHouseClientProtocol',
    'ClickHouseSettings',
    '_get_clickhouse_settings',
    '_make_clickhouse_client',
    'create_origo_database',
    'get_clickhouse_settings',
    'make_clickhouse_client',
]

DEFAULT_CLICKHOUSE_HOST = 'clickhouse'
DEFAULT_CLICKHOUSE_PORT = 9000
DEFAULT_CLICKHOUSE_USER = 'default'


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@runtime_checkable
class ClickHouseClientProtocol(Protocol):
    def execute(
        self,
        query: str,
        params: object | None = None,
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
    value = os.environ.get('CLICKHOUSE_PORT', str(DEFAULT_CLICKHOUSE_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_PORT environment variable must be an integer.') from exc


def _get_clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        host=os.environ.get('CLICKHOUSE_HOST', DEFAULT_CLICKHOUSE_HOST),
        port=_get_clickhouse_port(),
        user=os.environ.get('CLICKHOUSE_USER', DEFAULT_CLICKHOUSE_USER),
        password=_require_env('CLICKHOUSE_PASSWORD'),
        database=os.environ.get('CLICKHOUSE_DATABASE', 'origo'),
    )


def _make_clickhouse_client(settings: ClickHouseSettings) -> ClickhouseClient:
    return ClickhouseClient(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
    )


def get_clickhouse_settings() -> ClickHouseSettings:
    return _get_clickhouse_settings()


def make_clickhouse_client(settings: ClickHouseSettings) -> ClickHouseClientProtocol:
    client = _make_clickhouse_client(settings)
    if not isinstance(client, ClickHouseClientProtocol):
        raise TypeError('clickhouse_driver.Client does not satisfy the ClickHouse client contract.')
    return client


@asset(
    group_name='origo_setup',
    description='Creates the origo database if it does not already exist',
)
def create_origo_database(context: AssetExecutionContext) -> dict[str, object]:
    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        result = client.execute(f"SHOW DATABASES LIKE '{settings.database}'")
        if result:
            return {
                'database_created': False,
                'database_name': settings.database,
                'message': 'Database already exists',
            }

        client.execute(
            f"""
            CREATE DATABASE IF NOT EXISTS {settings.database}
            ENGINE = Atomic
            """
        )
        context.log.info(f'Created database {settings.database}.')
        return {'database_created': True, 'database_name': settings.database}
    finally:
        client.disconnect()
