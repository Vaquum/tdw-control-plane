import csv
import hashlib
import os
import zipfile
from datetime import date, datetime, timedelta, timezone
from io import BytesIO

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset

from .create_binance_futures_trades_table_origo import (
    LEDGER_TABLE_NAME,
    RAW_TABLE_NAME,
    create_binance_daily_futures_trades_table_origo,
)
from .create_origo_database import _get_clickhouse_settings, _make_clickhouse_client

DEFAULT_BINANCE_FUTURES_DAILY_TRADES_BASE_URL = (
    'https://data.binance.vision/data/futures/um/daily/trades/BTCUSDT/'
)
FUTURES_HEADER = ['id', 'price', 'qty', 'quote_qty', 'time', 'is_buyer_maker']
daily_partitions = DailyPartitionsDefinition(start_date='2019-09-08')


def _get_daily_futures_trades_base_url() -> str:
    return os.environ.get(
        'BINANCE_FUTURES_DAILY_TRADES_BASE_URL',
        DEFAULT_BINANCE_FUTURES_DAILY_TRADES_BASE_URL,
    )


def _download_bytes(url: str) -> bytes:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def _download_text(url: str) -> str:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def _datetime_from_timestamp(timestamp: int) -> datetime:
    if len(str(timestamp)) != 13:
        raise ValueError(f'Invalid timestamp length: {timestamp}')

    return datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def _extract_csv(zip_data: bytes) -> tuple[str, bytes]:
    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        with zip_ref.open(csv_filename) as csv_file:
            return csv_filename, csv_file.read()


def _parse_trade_row(row: list[str]) -> tuple[object, ...]:
    if len(row) != 6:
        raise ValueError(f'Unexpected Binance futures row shape: {row}')

    futures_trade_id = int(row[0])
    price = float(row[1])
    quantity = float(row[2])
    quote_quantity = float(row[3])
    timestamp = int(row[4])
    is_buyer_maker = row[5].lower() == 'true'

    return (
        futures_trade_id,
        price,
        quantity,
        quote_quantity,
        timestamp,
        is_buyer_maker,
        _datetime_from_timestamp(timestamp),
    )


def _parse_trade_rows(csv_content: bytes) -> list[tuple[object, ...]]:
    reader = csv.reader(csv_content.decode('utf-8').splitlines())
    rows: list[tuple[object, ...]] = []

    first_row = next(reader, None)
    if first_row is None:
        raise ValueError('Binance futures daily archive was empty')

    if first_row != FUTURES_HEADER:
        rows.append(_parse_trade_row(first_row))

    for row in reader:
        rows.append(_parse_trade_row(row))

    return rows


def _delete_partition_rows(
    client: ClickhouseClient,
    database: str,
    table_name: str,
    date_str: str,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{table_name}
        DELETE WHERE toDate(datetime) = toDate('{date_str}')
        """,
        settings={'mutations_sync': 2},
    )


def _delete_ledger_row(
    client: ClickhouseClient,
    database: str,
    source_date: str,
    source_file: str,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{LEDGER_TABLE_NAME}
        DELETE WHERE source_date = toDate('{source_date}')
          AND source_file = '{source_file}'
        """,
        settings={'mutations_sync': 2},
    )


def _count_rows_for_day(client: ClickhouseClient, database: str, date_str: str) -> int:
    result = client.execute(
        f"""
        SELECT count(*)
        FROM {database}.{RAW_TABLE_NAME}
        WHERE toDate(datetime) = toDate('{date_str}')
        """
    )
    return int(result[0][0])


def _write_ingestion_ledger(
    client: ClickhouseClient,
    database: str,
    *,
    source_date: str,
    source_file: str,
    dagster_run_id: str,
    dagster_partition_key: str,
    zip_checksum: str,
    csv_checksum: str,
    source_row_count: int,
    inserted_row_count: int,
) -> None:
    _delete_ledger_row(client, database, source_date, source_file)
    client.execute(
        f"""
        INSERT INTO {database}.{LEDGER_TABLE_NAME}
        (
            source_date,
            source_file,
            dagster_run_id,
            dagster_partition_key,
            zip_checksum,
            csv_checksum,
            source_row_count,
            inserted_row_count,
            loaded_at,
            status
        ) VALUES
        """,
        [
            (
                date.fromisoformat(source_date),
                source_file,
                dagster_run_id,
                dagster_partition_key,
                zip_checksum,
                csv_checksum,
                source_row_count,
                inserted_row_count,
                datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0),
                'success',
            )
        ],
    )


def _process_day(
    context: AssetExecutionContext,
    *,
    day_filename: str,
    date_str: str,
) -> dict[str, object]:
    base_url = _get_daily_futures_trades_base_url()
    file_url = f'{base_url}{day_filename}'
    checksum_url = f'{file_url}.CHECKSUM'

    context.log.info(f'Downloading checksum from {checksum_url}')
    checksum_text = _download_text(checksum_url)
    expected_checksum = checksum_text.split()[0].strip()

    context.log.info(f'Downloading trade data from {file_url}')
    zip_data = _download_bytes(file_url)
    actual_checksum = hashlib.sha256(zip_data).hexdigest()
    if actual_checksum != expected_checksum:
        raise ValueError(
            f'Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}'
        )

    csv_filename, csv_content = _extract_csv(zip_data)
    csv_checksum = hashlib.sha256(csv_content).hexdigest()
    rows = _parse_trade_rows(csv_content)

    settings = _get_clickhouse_settings()
    client = _make_clickhouse_client(settings)

    try:
        existing_count = _count_rows_for_day(client, settings.database, date_str)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing rows for {date_str}. Replacing that day.'
            )
            _delete_partition_rows(client, settings.database, RAW_TABLE_NAME, date_str)

        client.execute(
            f"""
            INSERT INTO {settings.database}.{RAW_TABLE_NAME}
            (
                futures_trade_id,
                price,
                quantity,
                quote_quantity,
                timestamp,
                is_buyer_maker,
                datetime
            ) SETTINGS async_insert=1, wait_for_async_insert=1
            VALUES
            """,
            rows,
            settings={'max_execution_time': 900},
        )

        inserted_count = _count_rows_for_day(client, settings.database, date_str)
        if inserted_count != len(rows):
            raise ValueError(
                f'Row count mismatch! Expected: {len(rows)}, Actual: {inserted_count}'
            )

        partition_key = context.partition_key or date_str
        _write_ingestion_ledger(
            client,
            settings.database,
            source_date=date_str,
            source_file=day_filename,
            dagster_run_id=context.run.run_id,
            dagster_partition_key=partition_key,
            zip_checksum=actual_checksum,
            csv_checksum=csv_checksum,
            source_row_count=len(rows),
            inserted_row_count=inserted_count,
        )

        return {
            'date': day_filename,
            'rows_inserted': inserted_count,
            'zip_checksum': actual_checksum,
            'csv_checksum': csv_checksum,
            'source_file': day_filename,
            'csv_file': csv_filename,
        }
    finally:
        client.disconnect()


@asset(
    partitions_def=daily_partitions,
    deps=[create_binance_daily_futures_trades_table_origo],
    group_name='binance_futures_data',
    description='Downloads, validates, extracts, and loads Binance BTCUSDT futures trade data into Origo',
)
def insert_daily_binance_futures_trades_to_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date_str = context.partition_key
    if partition_date_str is None:
        target_date = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = target_date.strftime('%Y-%m-%d')
    else:
        date_str = partition_date_str

    day_filename = f'BTCUSDT-trades-{date_str}.zip'
    context.log.info(f'Processing partition {date_str} using {day_filename}')
    return _process_day(context, day_filename=day_filename, date_str=date_str)
