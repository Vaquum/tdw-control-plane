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
