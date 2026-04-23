from __future__ import annotations

import csv
import hashlib
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
BINANCE_FIXTURE_ROOT = REPO_ROOT / 'tests' / 'fixtures' / 'binance'
ORIGO_DATABASE = 'origo'
BINANCE_SPOT_DATASET_SOURCE = 'binance_spot'

KLINE_SCHEMA_COLUMNS = [
    ('open_time', 'UInt64'),
    ('open', 'Float64'),
    ('high', 'Float64'),
    ('low', 'Float64'),
    ('close', 'Float64'),
    ('volume', 'Float64'),
    ('close_time', 'UInt64'),
    ('quote_asset_volume', 'Float64'),
    ('number_of_trades', 'UInt64'),
    ('taker_buy_base_asset_volume', 'Float64'),
    ('taker_buy_quote_asset_volume', 'Float64'),
    ('ignore', 'Float64'),
]

ALIGNED_SCHEMA_COLUMNS = [
    ('dataset_source', 'LowCardinality(String)'),
    *KLINE_SCHEMA_COLUMNS,
]


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
        with archive.open(archive.namelist()[0]) as csv_file:
            csv_bytes = csv_file.read()

    rows: list[tuple[Any, ...]] = []
    reader = csv.reader(csv_bytes.decode('utf-8').splitlines())
    for row in reader:
        raw_timestamp = row[4]
        timestamp = int(raw_timestamp)
        timestamp_length = len(raw_timestamp)
        if timestamp_length == 13:
            dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).replace(
                tzinfo=None
            )
        elif timestamp_length == 16:
            dt = datetime.fromtimestamp(timestamp / 1000000.0, tz=timezone.utc).replace(
                tzinfo=None
            )
        else:
            raise ValueError(
                f'Unsupported fixture timestamp length {timestamp_length} for value {raw_timestamp}'
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


def load_expected_binance_spot_kline_rows(date_str: str) -> list[tuple[Any, ...]]:
    trade_rows = load_expected_trade_rows(date_str)
    grouped: dict[datetime, list[tuple[Any, ...]]] = {}

    for row in trade_rows:
        dt = row[7]
        minute_start = dt.replace(second=0, microsecond=0)
        grouped.setdefault(minute_start, []).append(row)

    rows: list[tuple[Any, ...]] = []
    for minute_start in sorted(grouped):
        minute_rows = sorted(grouped[minute_start], key=lambda row: row[0])
        open_time = int(minute_start.replace(tzinfo=timezone.utc).timestamp() * 1000)
        close_time = open_time + 60000 - 1
        rows.append(
            (
                open_time,
                minute_rows[0][1],
                max(row[1] for row in minute_rows),
                min(row[1] for row in minute_rows),
                minute_rows[-1][1],
                sum(row[2] for row in minute_rows),
                close_time,
                sum(row[3] for row in minute_rows),
                len(minute_rows),
                sum(row[2] for row in minute_rows if row[5] == 0),
                sum(row[3] for row in minute_rows if row[5] == 0),
                0.0,
            )
        )

    return rows


def load_expected_aligned_1m_exchange_rows(date_str: str) -> list[tuple[Any, ...]]:
    return [
        (BINANCE_SPOT_DATASET_SOURCE, *row)
        for row in load_expected_binance_spot_kline_rows(date_str)
    ]
