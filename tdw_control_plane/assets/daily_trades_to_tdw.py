import os
import csv
import requests
import zipfile
import hashlib
from datetime import datetime, timedelta
from io import BytesIO
from clickhouse_driver import Client as ClickhouseClient
from dagster import asset, DailyPartitionsDefinition

# Create a daily partition definition
daily_partitions = DailyPartitionsDefinition(
    start_date="2017-08-17"
)

# Configure Clickhouse connection
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "37.27.112.187")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'tdw')
CLICKHOUSE_TABLE = os.environ.get('CLICKHOUSE_TABLE', 'binance_trades')

def compute_sha256(data):
    """Compute SHA256 checksum of data."""
    if isinstance(data, bytes):
        return hashlib.sha256(data).hexdigest()
    else:
        # For file-like objects
        sha256_hash = hashlib.sha256()
        for byte_block in iter(lambda: data.read(4096), b""):
            sha256_hash.update(byte_block)
        data.seek(0)  # Reset file pointer
        return sha256_hash.hexdigest()

@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Downloads, validates, extracts, and loads Binance BTC trade data into Clickhouse"
)
def insert_daily_binance_trades_to_tdw(context):
    """
    Complete pipeline with validation at each step.
    """
    # Base URL for Binance data
    base_url = "https://data.binance.vision/data/spot/daily/trades/BTCUSDT/"
    
    # Get date from partition
    partition_date_str = context.asset_partition_key_for_output()
    
    if partition_date_str is None:
        # If no partition specified, default to yesterday
        target_date = datetime.now() - timedelta(days=1)
        date_str = target_date.strftime("%Y-%m-%d")
    else:
        # Use the partition date
        date_str = partition_date_str
    
    # Construct the filename and URLs
    filename = f"BTCUSDT-trades-{date_str}.zip"
    file_url = base_url + filename
    checksum_url = file_url + ".CHECKSUM"
    
    # 1. Download the checksum file
    context.log.info(f"Downloading checksum from {checksum_url}")
    checksum_response = requests.get(checksum_url)
    checksum_response.raise_for_status()
    
    # Parse the checksum (format: "SHA256  filename = checksum")
    expected_checksum = checksum_response.text.split()[-1].strip()
    context.log.info(f"Expected SHA256: {expected_checksum}")
    
    # 2. Download the zip file
    context.log.info(f"Downloading {filename} from {file_url}")
    response = requests.get(file_url)
    response.raise_for_status()
    zip_data = response.content
    
    # 3. Verify zip file checksum
    actual_checksum = hashlib.sha256(zip_data).hexdigest()
    if actual_checksum != expected_checksum:
        raise ValueError(f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}")
    context.log.info("Zip file checksum verified successfully")
    
    # 4. Extract the CSV file
    context.log.info("Extracting CSV file")
    csv_content = None
    csv_filename = None
    
    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        # Get the CSV filename inside the zip
        csv_filename = zip_ref.namelist()[0]
        
        # Extract the CSV content
        with zip_ref.open(csv_filename) as csv_file:
            csv_content = csv_file.read()
    
    # 5. Compute checksum for extracted CSV
    csv_checksum = hashlib.sha256(csv_content).hexdigest()
    context.log.info(f"CSV checksum: {csv_checksum}")
    
    # 6. Parse CSV data
    context.log.info("Parsing CSV data")
    data = []
    
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    headers = next(reader)
    
    for row in reader:
        # Parse the row data
        trade_id = int(row[0])
        price = float(row[1])
        quantity = float(row[2])
        quote_quantity = float(row[3])
        timestamp = int(row[4])
        is_buyer_maker = row[5].lower() == 'true'
        is_best_match = row[6].lower() == 'true'
        
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(timestamp / 1000000.0)
        
        # Add to data list
        data.append((
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            is_best_match,
            dt
        ))
    
    # 7. Insert data into Clickhouse
    context.log.info(f"Inserting {len(data)} rows into Clickhouse")
    
    try:
        context.log.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            compression=False  # Disable compression to avoid clickhouse-cityhash dependency
        )
        
        # Check if data already exists for this day
        result = client.execute(
            f"""
            SELECT count(*) 
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE toDate(datetime) = toDate('{date_str}')
            """
        )
        inserted_count = result[0][0]
        
        # Insert data
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
            ) VALUES
            """,
            data
        )
        
        # 8. Verify insertion
        # Count rows for this date
        result = client.execute(
            f"""
            SELECT count(*) 
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE toDate(datetime) = toDate('{date_str}')
            """
        )
        inserted_count = result[0][0]
        
        # Compute a checksum for the inserted data
        checksum_result = client.execute(
            f"""
            SELECT cityHash64(groupArray(toString(trade_id)), groupArray(toString(price)))
            FROM (
                SELECT trade_id, price
                FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
                WHERE toDate(datetime) = toDate('{date_str}')
                ORDER BY trade_id
            )
            """
        )
        data_checksum = checksum_result[0][0]
        
        context.log.info(f"Successfully inserted {inserted_count} rows with checksum {data_checksum}")
        
        # Verify the count matches
        if inserted_count != len(data):
            raise ValueError(f"Row count mismatch! Expected: {len(data)}, Actual: {inserted_count}")
        
        return {
            "date": date_str,
            "rows_inserted": inserted_count,
            "zip_checksum": actual_checksum,
            "csv_checksum": csv_checksum,
            "data_checksum": data_checksum
        }
        
    except Exception as e:
        context.log.error(f"Error during Clickhouse operation: {e}")
        # Optionally, perform cleanup if needed
        raise e
