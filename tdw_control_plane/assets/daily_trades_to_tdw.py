import csv
import hashlib
import os
import tempfile
import zipfile
from datetime import datetime, timedelta
from io import TextIOWrapper

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import DailyPartitionsDefinition, asset
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "binance_daily_trades")

DOWNLOAD_TIMEOUT = (30, 300)
DOWNLOAD_CHUNK_SIZE = 1024 * 1024
INSERT_BATCH_SIZE = 100_000

daily_partitions = DailyPartitionsDefinition(start_date="2017-08-17")


def _get_clickhouse_password():
    if not CLICKHOUSE_PASSWORD:
        raise RuntimeError(
            "CLICKHOUSE_PASSWORD environment variable must be set before creating the ClickHouse client."
        )

    return CLICKHOUSE_PASSWORD


def _build_requests_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _download_to_tempfile(session: requests.Session, url: str) -> tuple[str, int, str]:
    downloaded_bytes = 0
    zip_checksum = hashlib.sha256()
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")

    try:
        with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if not chunk:
                    continue

                downloaded_bytes += len(chunk)
                zip_checksum.update(chunk)
                tmp_file.write(chunk)
    except Exception:
        tmp_file.close()
        os.unlink(tmp_file.name)
        raise

    tmp_file.close()
    return tmp_file.name, downloaded_bytes, zip_checksum.hexdigest()


def _compute_csv_checksum(zip_path: str, csv_filename: str) -> str:
    csv_checksum = hashlib.sha256()

    with zipfile.ZipFile(zip_path) as zip_ref:
        with zip_ref.open(csv_filename) as csv_file:
            while chunk := csv_file.read(DOWNLOAD_CHUNK_SIZE):
                csv_checksum.update(chunk)

    return csv_checksum.hexdigest()


def _normalize_datetime_seconds(timestamp: int) -> int:
    timestamp_len = len(str(timestamp))
    if timestamp_len == 13:
        return timestamp // 1_000
    if timestamp_len == 16:
        return timestamp // 1_000_000

    raise ValueError(f"Invalid timestamp length: {timestamp}")


def _insert_batch(client: ClickhouseClient, batch: list[tuple]):
    client.execute(
        f"""
        INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
        (
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            is_best_match,
            datetime
        ) SETTINGS async_insert=1, wait_for_async_insert=1
        VALUES
        """,
        batch,
        settings={"max_execution_time": 900},
    )


def _insert_csv_batches(
    context,
    client: ClickhouseClient,
    zip_path: str,
    csv_filename: str,
) -> int:
    row_count = 0
    batch = []

    with zipfile.ZipFile(zip_path) as zip_ref:
        with zip_ref.open(csv_filename) as csv_file:
            reader = csv.reader(TextIOWrapper(csv_file, encoding="utf-8", newline=""))

            for row in reader:
                row_count += 1

                trade_id = int(row[0])
                price = float(row[1])
                quantity = float(row[2])
                quote_quantity = float(row[3])
                timestamp = int(row[4])
                is_buyer_maker = row[5].lower() == "true"
                is_best_match = row[6].lower() == "true"
                datetime_seconds = _normalize_datetime_seconds(timestamp)

                batch.append(
                    (
                        trade_id,
                        price,
                        quantity,
                        quote_quantity,
                        timestamp,
                        is_buyer_maker,
                        is_best_match,
                        datetime_seconds,
                    )
                )

                if len(batch) >= INSERT_BATCH_SIZE:
                    _insert_batch(client, batch)
                    context.log.info(
                        f"Inserted batch of {len(batch)} rows for {CLICKHOUSE_TABLE}."
                    )
                    batch = []

    if batch:
        _insert_batch(client, batch)
        context.log.info(f"Inserted final batch of {len(batch)} rows for {CLICKHOUSE_TABLE}.")

    return row_count


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Downloads, validates, extracts, and loads daily Binance BTC trade data into TDW",
)
def insert_daily_binance_trades_to_tdw(context):
    partition_date_str = context.asset_partition_key_for_output()

    if partition_date_str is None:
        target_date = datetime.now() - timedelta(days=1)
        date_str = target_date.strftime("%Y-%m-%d")
    else:
        date_str = partition_date_str

    day_str = f"BTCUSDT-trades-{date_str}.zip"
    context.log.info(
        f"Processing selected partition: {partition_date_str}, file: {day_str}"
    )

    return _process_day(context, day_str, date_str)


def _process_day(context, day_str, date_str):
    base_url = "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
    file_url = base_url + day_str
    checksum_url = file_url + ".CHECKSUM"

    session = _build_requests_session()
    zip_path = None
    client = None

    try:
        context.log.info(f"Downloading checksum from {checksum_url}")
        checksum_response = session.get(checksum_url, timeout=DOWNLOAD_TIMEOUT)
        checksum_response.raise_for_status()
        expected_checksum = checksum_response.text.split()[0].strip()
        context.log.info(f"Expected checksum: {expected_checksum}")

        context.log.info(f"Downloading trade data from {file_url}")
        zip_path, downloaded_bytes, actual_checksum = _download_to_tempfile(session, file_url)
        context.log.info(f"Downloaded {downloaded_bytes / 1024 / 1024:.2f} MB of data")
        context.log.info(f"Actual checksum: {actual_checksum}")

        if actual_checksum != expected_checksum:
            context.log.error(
                f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}"
            )
            raise ValueError(
                f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}"
            )

        with zipfile.ZipFile(zip_path) as zip_ref:
            csv_filename = zip_ref.namelist()[0]
            context.log.info(f"Found CSV file: {csv_filename}")

        csv_checksum = _compute_csv_checksum(zip_path, csv_filename)
        context.log.info(f"CSV checksum: {csv_checksum}")

        context.log.info(
            f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
        )
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=_get_clickhouse_password(),
            database=CLICKHOUSE_DATABASE,
            compression=True,
            send_receive_timeout=900,
        )

        context.log.info(f"Checking for existing data for {date_str}")
        check_result = client.execute(
            f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE toDate(datetime) = toDate('{date_str}')
        """
        )
        existing_count = check_result[0][0]

        if existing_count > 0:
            context.log.info(
                f"Found {existing_count} existing records for {date_str}. Deleting before reinserting."
            )
            client.execute(
                f"""
                ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
                DELETE WHERE toDate(datetime) = toDate('{date_str}')
            """
            )
            context.log.info(f"Deleted existing data for {date_str}")

        context.log.info("Parsing CSV data and inserting batches into ClickHouse")
        row_count = _insert_csv_batches(context, client, zip_path, csv_filename)
        context.log.info(f"Parsed and inserted {row_count} rows from CSV")

        context.log.info("Verifying data insertion")
        result = client.execute(
            f"""
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE toDate(datetime) = toDate('{date_str}')
        """
        )
        inserted_count = result[0][0]
        context.log.info(f"Found {inserted_count} rows in ClickHouse after insertion")

        context.log.info("Computing verification statistics")
        stats_result = client.execute(
            f"""
            SELECT
                min(trade_id),
                max(trade_id),
                avg(price),
                count(distinct trade_id) % 1000
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE toDate(datetime) = toDate('{date_str}')
        """
        )

        data_verification = {
            "min_trade_id": stats_result[0][0],
            "max_trade_id": stats_result[0][1],
            "avg_price": stats_result[0][2],
            "id_uniqueness_check": stats_result[0][3],
        }
        context.log.info(f"Data verification stats: {data_verification}")

        if inserted_count != row_count:
            context.log.error(
                f"Row count mismatch! Expected: {row_count}, Actual: {inserted_count}"
            )
            raise ValueError(
                f"Row count mismatch! Expected: {row_count}, Actual: {inserted_count}"
            )

        result_data = {
            "date": day_str,
            "rows_inserted": inserted_count,
            "zip_checksum": actual_checksum,
            "csv_checksum": csv_checksum,
            "data_verification": data_verification,
        }

        context.log.info(f"Successfully processed {day_str}")
        return result_data

    except Exception:
        raise
    finally:
        session.close()
        if client:
            try:
                client.disconnect()
            except Exception:
                pass
        if zip_path and os.path.exists(zip_path):
            os.unlink(zip_path)
