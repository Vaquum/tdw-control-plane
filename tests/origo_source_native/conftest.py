from __future__ import annotations

import importlib
import shutil
import socket
import subprocess
import time
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any
from uuid import uuid4

import pytest
from clickhouse_driver import Client as ClickhouseClient
from dagster import materialize

from .helpers import BINANCE_FIXTURE_ROOT, ORIGO_DATABASE

REPO_ROOT = Path(__file__).resolve().parents[2]
CLICKHOUSE_IMAGE = 'clickhouse/clickhouse-server:24.3'


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


def _wait_for_clickhouse(host: str, port: int, user: str, password: str) -> None:
    deadline = time.time() + 60
    last_error: Exception | None = None
    while time.time() < deadline:
        client = None
        try:
            client = ClickhouseClient(
                host=host,
                port=port,
                user=user,
                password=password,
            )
            result = client.execute('SELECT 1')
            if result == [(1,)]:
                return
        except OSError as exc:
            last_error = exc
            time.sleep(1)
        except RuntimeError as exc:
            last_error = exc
            time.sleep(1)
        except ValueError as exc:
            last_error = exc
            time.sleep(1)
        except EOFError as exc:
            last_error = exc
            time.sleep(1)
        finally:
            if client is not None:
                client.disconnect()
    raise RuntimeError(f'ClickHouse container did not become ready: {last_error}')


def _clickhouse_env(native_port: int, password: str) -> dict[str, str]:
    return {
        'CLICKHOUSE_HOST': '127.0.0.1',
        'CLICKHOUSE_PORT': str(native_port),
        'CLICKHOUSE_USER': 'default',
        'CLICKHOUSE_PASSWORD': password,
        'CLICKHOUSE_DATABASE': ORIGO_DATABASE,
    }


def _make_admin_client(settings: dict[str, str]) -> ClickhouseClient:
    return ClickhouseClient(
        host=settings['CLICKHOUSE_HOST'],
        port=int(settings['CLICKHOUSE_PORT']),
        user=settings['CLICKHOUSE_USER'],
        password=settings['CLICKHOUSE_PASSWORD'],
    )


def _drop_origo_database(settings: dict[str, str]) -> None:
    client = _make_admin_client(settings)
    try:
        client.execute(f'DROP DATABASE IF EXISTS {ORIGO_DATABASE} SYNC')
    finally:
        client.disconnect()


def _query_rows(settings: dict[str, str], query: str) -> list[tuple[Any, ...]]:
    client = ClickhouseClient(
        host=settings['CLICKHOUSE_HOST'],
        port=int(settings['CLICKHOUSE_PORT']),
        user=settings['CLICKHOUSE_USER'],
        password=settings['CLICKHOUSE_PASSWORD'],
        database=ORIGO_DATABASE,
    )
    try:
        return client.execute(query)
    finally:
        client.disconnect()
@pytest.fixture(scope='session')
def clickhouse_settings() -> dict[str, str]:
    if shutil.which('docker') is None:
        pytest.fail('docker CLI is required for tests/origo_source_native')

    container_name = f'tdw-origo-tests-{uuid4().hex[:12]}'
    native_port = _free_port()
    password = 'test-password'
    settings = _clickhouse_env(native_port, password)

    subprocess.run(
        [
            'docker',
            'run',
            '--detach',
            '--rm',
            '--name',
            container_name,
            '--tmpfs',
            '/var/lib/clickhouse:size=512m',
            '--tmpfs',
            '/var/log/clickhouse-server:size=64m',
            '--publish',
            f'127.0.0.1:{native_port}:9000',
            '--env',
            'CLICKHOUSE_USER=default',
            '--env',
            f'CLICKHOUSE_PASSWORD={password}',
            CLICKHOUSE_IMAGE,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    try:
        _wait_for_clickhouse(
            settings['CLICKHOUSE_HOST'],
            int(settings['CLICKHOUSE_PORT']),
            settings['CLICKHOUSE_USER'],
            settings['CLICKHOUSE_PASSWORD'],
        )
        yield settings
    finally:
        subprocess.run(
            ['docker', 'rm', '--force', container_name],
            check=False,
            capture_output=True,
            text=True,
        )


@pytest.fixture(scope='session')
def binance_daily_base_url() -> str:
    port = _free_port()
    handler = partial(SimpleHTTPRequestHandler, directory=str(BINANCE_FIXTURE_ROOT))
    server = ThreadingHTTPServer(('127.0.0.1', port), handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield f'http://127.0.0.1:{port}/spot/daily/trades/BTCUSDT/'
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture()
def origo_test_env(
    monkeypatch: pytest.MonkeyPatch,
    clickhouse_settings: dict[str, str],
    binance_daily_base_url: str,
) -> dict[str, str]:
    _drop_origo_database(clickhouse_settings)
    for key, value in clickhouse_settings.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv('BINANCE_SPOT_DAILY_TRADES_BASE_URL', binance_daily_base_url)

    yield clickhouse_settings

    _drop_origo_database(clickhouse_settings)


@pytest.fixture()
def origo_assets(origo_test_env: dict[str, str]) -> dict[str, Any]:
    create_origo_database_module = importlib.import_module(
        'tdw_control_plane.assets.create_origo_database'
    )
    create_binance_trades_table_origo_module = importlib.import_module(
        'tdw_control_plane.assets.create_binance_trades_table_origo'
    )
    daily_trades_to_origo_module = importlib.import_module(
        'tdw_control_plane.assets.daily_trades_to_origo'
    )

    return {
        'create_origo_database': create_origo_database_module.create_origo_database,
        'create_binance_daily_spot_trades_table_origo': (
            create_binance_trades_table_origo_module.create_binance_daily_spot_trades_table_origo
        ),
        'insert_daily_binance_spot_trades_to_origo': (
            daily_trades_to_origo_module.insert_daily_binance_spot_trades_to_origo
        ),
        'RAW_TABLE_NAME': create_binance_trades_table_origo_module.RAW_TABLE_NAME,
        'LEDGER_TABLE_NAME': create_binance_trades_table_origo_module.LEDGER_TABLE_NAME,
    }


@pytest.fixture()
def materialize_origo_assets(
    origo_assets: dict[str, Any],
) -> Any:
    def _run(*, partition_key: str | None = None) -> Any:
        return materialize(
            [
                origo_assets['create_origo_database'],
                origo_assets['create_binance_daily_spot_trades_table_origo'],
                origo_assets['insert_daily_binance_spot_trades_to_origo'],
            ],
            partition_key=partition_key,
        )

    return _run


@pytest.fixture()
def query_origo(clickhouse_settings: dict[str, str]) -> Any:
    def _run(query: str) -> list[tuple[Any, ...]]:
        return _query_rows(clickhouse_settings, query)

    return _run
