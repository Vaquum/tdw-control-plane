from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import socket
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Thread
from typing import Any
from uuid import uuid4

import pytest
from clickhouse_driver import Client as ClickhouseClient
from dagster import materialize

os.environ.setdefault('CLICKHOUSE_HOST', '127.0.0.1')
os.environ.setdefault('CLICKHOUSE_PORT', '9000')
os.environ.setdefault('CLICKHOUSE_USER', 'default')
os.environ.setdefault('CLICKHOUSE_PASSWORD', 'test-password')

from tdw_control_plane.assets.create_binance_trades_table_origo import (
    LEDGER_TABLE_NAME,
    RAW_TABLE_NAME,
    create_binance_daily_spot_trades_table_origo,
)
from tdw_control_plane.assets.create_origo_database import create_origo_database
from tdw_control_plane.assets.daily_trades_to_origo import (
    insert_daily_binance_spot_trades_to_origo,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
BINANCE_FIXTURE_ROOT = REPO_ROOT / 'tests' / 'fixtures' / 'binance'
CLICKHOUSE_IMAGE = 'clickhouse/clickhouse-server:24.3'
ORIGO_DATABASE = 'origo'


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
        except Exception as exc:
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


def _load_fixture_zip_bytes(date_str: str) -> bytes:
    path = (
        BINANCE_FIXTURE_ROOT
        / 'spot'
        / 'daily'
        / 'trades'
        / 'BTCUSDT'
        / f'BTCUSDT-trades-{date_str}.zip'
    )
    return path.read_bytes()


def load_expected_trade_rows(date_str: str) -> list[tuple[Any, ...]]:
    zip_bytes = _load_fixture_zip_bytes(date_str)
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        csv_name = archive.namelist()[0]
        with archive.open(csv_name) as csv_file:
            csv_bytes = csv_file.read()

    rows: list[tuple[Any, ...]] = []
    reader = csv.reader(csv_bytes.decode('utf-8').splitlines())
    for row in reader:
        timestamp = int(row[4])
        if len(str(timestamp)) == 13:
            dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).replace(
                tzinfo=None
            )
        else:
            dt = datetime.fromtimestamp(timestamp / 1000000.0, tz=timezone.utc).replace(
                tzinfo=None
            )
        rows.append(
            (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                timestamp,
                1 if row[5].lower() == 'true' else 0,
                1 if row[6].lower() == 'true' else 0,
                dt,
            )
        )
    return rows


def load_expected_ledger_payload(date_str: str) -> dict[str, Any]:
    zip_bytes = _load_fixture_zip_bytes(date_str)
    zip_checksum = hashlib.sha256(zip_bytes).hexdigest()
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        with archive.open(archive.namelist()[0]) as csv_file:
            csv_bytes = csv_file.read()

    return {
        'source_date': date_str,
        'source_file': f'BTCUSDT-trades-{date_str}.zip',
        'zip_checksum': zip_checksum,
        'csv_checksum': hashlib.sha256(csv_bytes).hexdigest(),
        'source_row_count': len(load_expected_trade_rows(date_str)),
    }


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
def materialize_origo_assets(origo_test_env: dict[str, str]):
    def _run(*, partition_key: str | None = None):
        return materialize(
            [
                create_origo_database,
                create_binance_daily_spot_trades_table_origo,
                insert_daily_binance_spot_trades_to_origo,
            ],
            partition_key=partition_key,
        )

    return _run


@pytest.fixture()
def query_origo(clickhouse_settings: dict[str, str]):
    def _run(query: str) -> list[tuple[Any, ...]]:
        return _query_rows(clickhouse_settings, query)

    return _run


def dump_ruleset_contexts() -> str:
    path = REPO_ROOT / '.github' / 'rulesets' / 'main.json'
    payload = json.loads(path.read_text(encoding='utf-8'))
    checks = next(
        rule['parameters']['required_status_checks']
        for rule in payload['rules']
        if rule['type'] == 'required_status_checks'
    )
    return '\n'.join(entry['context'] for entry in checks)
