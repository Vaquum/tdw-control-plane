import os
import csv
import requests
import zipfile
import hashlib
from datetime import datetime, timedelta
from io import BytesIO
from clickhouse_driver import Client as ClickhouseClient
from dagster import asset, DailyPartitionsDefinition

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "binance_daily_trades")

daily_partitions = DailyPartitionsDefinition(start_date="2017-08-17")


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

    context.log.info(f"Downloading checksum from {checksum_url}")
    checksum_response = requests.get(checksum_url)
    checksum_response.raise_for_status()

    expected_checksum = checksum_response.text.split()[0].strip()
    context.log.info(f"Expected checksum: {expected_checksum}")

    context.log.info(f"Downloading trade data from {file_url}")
    response = requests.get(file_url)
    response.raise_for_status()
    zip_data = response.content
    context.log.info(f"Downloaded {len(zip_data) / 1024 / 1024:.2f} MB of data")

    actual_checksum = hashlib.sha256(zip_data).hexdigest()
    context.log.info(f"Actual checksum: {actual_checksum}")
    if actual_checksum != expected_checksum:
        context.log.error(
            f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}"
        )
        raise ValueError(
            f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}"
        )

    csv_content = None

    context.log.info("Extracting CSV from zip file")
    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        context.log.info(f"Found CSV file: {csv_filename}")

        with zip_ref.open(csv_filename) as csv_file:
            csv_content = csv_file.read()

    csv_checksum = hashlib.sha256(csv_content).hexdigest()
    context.log.info(f"CSV checksum: {csv_checksum}")

    context.log.info("Parsing CSV data")
    data = []

    csv_text = csv_content.decode("utf-8")
    reader = csv.reader(csv_text.splitlines())

    row_count = 0
    for row in reader:
        row_count += 1
        trade_id = int(row[0])
        price = float(row[1])
        quantity = float(row[2])
        quote_quantity = float(row[3])
        timestamp = int(row[4])
        is_buyer_maker = row[5].lower() == "true"
        is_best_match = row[6].lower() == "true"

        if len(str(timestamp)) == 13:
            dt = datetime.fromtimestamp(timestamp / 1000.0)
        elif len(str(timestamp)) == 16:
            dt = datetime.fromtimestamp(timestamp / 1000000.0)
        else:
            raise ValueError(f"Invalid timestamp length: {timestamp}")

        data.append(
            (
                trade_id,
                price,
                quantity,
                quote_quantity,
                timestamp,
                is_buyer_maker,
                is_best_match,
                dt,
            )
        )

    context.log.info(f"Parsed {row_count} rows from CSV")

    csv_text = None
    csv_content = None
    zip_data = None

    client = None
    try:
        context.log.info(
            f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"
        )
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
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

        context.log.info(f"Inserting {len(data)} rows into ClickHouse")
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
            data,
            settings={"max_execution_time": 900},
        )
        context.log.info("Data insertion completed")

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

        if inserted_count != len(data):
            context.log.error(
                f"Row count mismatch! Expected: {len(data)}, Actual: {inserted_count}"
            )
            raise ValueError(
                f"Row count mismatch! Expected: {len(data)}, Actual: {inserted_count}"
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

    except Exception as e:
        raise e
    finally:
        if client:
            try:
                client.disconnect()
            except Exception:
                pass
        data = None
