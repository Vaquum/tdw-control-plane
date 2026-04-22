import os
from dataclasses import dataclass

from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, asset


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


def _get_clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        host=_require_env('CLICKHOUSE_HOST'),
        port=int(_require_env('CLICKHOUSE_PORT')),
        user=_require_env('CLICKHOUSE_USER'),
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
