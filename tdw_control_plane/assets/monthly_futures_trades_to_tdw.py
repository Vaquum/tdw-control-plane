import os
import csv
import requests
import zipfile
import hashlib
from datetime import datetime, timedelta
from io import BytesIO
from clickhouse_driver import Client as ClickhouseClient
from dagster import asset, MonthlyPartitionsDefinition
from datetime import date

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'tdw')
CLICKHOUSE_TABLE = os.environ.get('CLICKHOUSE_TABLE', 'binance_futures_trades')

monthly_partitions = MonthlyPartitionsDefinition(
    start_date='2019-09-01'
)


@asset(
    partitions_def=monthly_partitions,
    group_name='binance_futures_trades_data',
    description='Downloads, validates, extracts, and loads Binance BTC futures trade data into Clickhouse'
)

def insert_monthly_binance_futures_trades_to_tdw(context):
    # Get the selected partition key (YYYY-MM-DD format)
    partition_date_str = context.asset_partition_key_for_output()
    
    # Extract just the year and month parts (first two elements)
    date_parts = partition_date_str.split('-')
    year, month = date_parts[0], date_parts[1]
    
    # Generate the month string for the selected partition
    month_str = f'BTCUSDT-trades-{year}-{month}.zip'
    context.log.info(f"Processing selected partition: {partition_date_str}, file: {month_str}")
    
    # Process only the selected month
    result = _process_month(context, month_str)
    
    return result

def _process_month(context, month_str):
    
    base_url = 'https://data.binance.vision/data/futures/um/monthly/trades/BTCUSDT/'
    file_url = base_url + month_str
    checksum_url = file_url + '.CHECKSUM'
    
    context.log.info(f"Downloading checksum from {checksum_url}")
    checksum_response = requests.get(checksum_url)
    checksum_response.raise_for_status()
    
    expected_checksum = checksum_response.text.split()[0].strip()
    context.log.info(f"Expected checksum: {expected_checksum}")

    context.log.info(f"Downloading trade data from {file_url}")
    response = requests.get(file_url)
    response.raise_for_status()
    zip_data = response.content
    context.log.info(f"Downloaded {len(zip_data)/1024/1024:.2f} MB of data")
    
    actual_checksum = hashlib.sha256(zip_data).hexdigest()
    context.log.info(f"Actual checksum: {actual_checksum}")
    
    if actual_checksum != expected_checksum:
        context.log.error(f"Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}")
        raise ValueError(f'Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}')
    
    csv_content = None
    csv_filename = None
    
    context.log.info("Extracting CSV from zip file")
    
    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        context.log.info(f"Found CSV file: {csv_filename}")
        
        with zip_ref.open(csv_filename) as csv_file:
            csv_content = csv_file.read()
    
    # Calculate CSV checksum
    csv_checksum = hashlib.sha256(csv_content).hexdigest()
    context.log.info(f"CSV checksum: {csv_checksum}")
    
    # Parse CSV data
    context.log.info("Parsing CSV data")
    data = []
    
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    headers = next(reader)
    
    row_count = 0
    for row in reader:
        row_count += 1
        futures_trade_id = int(row[0])
        price = float(row[1])
        quantity = float(row[2])
        quote_quantity = float(row[3])
        timestamp = int(row[4])
        is_buyer_maker = row[5].lower() == 'true'
        
        # Binance started with milliseconds, then switched to microseconds
        if len(str(timestamp)) == 13:
            dt = datetime.fromtimestamp(timestamp / 1000.0)
        
        elif len(str(timestamp)) == 16:
            dt = datetime.fromtimestamp(timestamp / 1000000.0)
        
        else:
            raise ValueError(f"Invalid timestamp length: {timestamp}")
        
        data.append((
            futures_trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            dt
        ))
    
    context.log.info(f"Parsed {row_count} rows from CSV")
    
    # Clear large variables to help garbage collection
    csv_text = None
    csv_content = None
    zip_data = None
    
    # Extract year and month from the filename for verification
    # Format: BTCUSDT-trades-YYYY-MM.zip
    year_month = month_str.split('-')[2:4]
    if len(year_month) >= 2:
        year, month = year_month[0], year_month[1].split('.')[0]
    else:
        # Handle old format: BTCUSDT-trades-YYYY-M.zip
        filename_parts = month_str.split('-')
        year = filename_parts[2]
        month = filename_parts[3].split('.')[0]
    
    month = month.zfill(2)
    month_start = f'{year}-{month}-01'
    context.log.info(f"Month start date: {month_start}")
    
    # Connect to ClickHouse
    client = None
    try:
        context.log.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            compression=True,
            send_receive_timeout=900,
        )
        
        # Check if data already exists for this month
        context.log.info(f"Checking for existing data for {month_start}")
        check_result = client.execute(f'''
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE datetime >= toDate('{month_start}')
            AND datetime < addMonths(toDate('{month_start}'), 1)
        ''')
        
        existing_count = check_result[0][0]
        
        # If data exists, delete it before inserting new data
        if existing_count > 0:
            context.log.info(f"Found {existing_count} existing records for {month_start}. Deleting before reinserting.")
            client.execute(f'''
                ALTER TABLE {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} 
                DELETE WHERE datetime >= toDate('{month_start}')
                AND datetime < addMonths(toDate('{month_start}'), 1)
            ''')
            context.log.info(f"Deleted existing data for {month_start}")
        
        # Insert data
        context.log.info(f"Inserting {len(data)} rows into ClickHouse")
        client.execute(
            f'''
            INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
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
            ''',
            data,
            settings={'max_execution_time': 900}
        )
        context.log.info("Data insertion completed")

        # Verify insertion
        context.log.info("Verifying data insertion")
        result = client.execute(f'''
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE datetime >= toDate('{month_start}')
            AND datetime <  addMonths(toDate('{month_start}'), 1)
        ''')
        inserted_count = result[0][0]
        context.log.info(f"Found {inserted_count} rows in ClickHouse after insertion")

        # Get quick stats instead of expensive hash
        context.log.info("Computing verification statistics")
        stats_result = client.execute(f'''
            SELECT 
                min(futures_trade_id),
                max(futures_trade_id),
                avg(price),
                count(distinct futures_trade_id) % 1000 -- lightweight uniqueness check (modulo to keep it small)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE datetime >= toDate('{month_start}')
            AND datetime <  addMonths(toDate('{month_start}'), 1)
        ''')
        
        data_verification = {
            'min_trade_id': stats_result[0][0],
            'max_trade_id': stats_result[0][1],
            'avg_price': stats_result[0][2],
            'id_uniqueness_check': stats_result[0][3]
        }
        context.log.info(f"Data verification stats: {data_verification}")
        
        if inserted_count != len(data):
            context.log.error(f"Row count mismatch! Expected: {len(data)}, Actual: {inserted_count}")
            raise ValueError(f'Row count mismatch! Expected: {len(data)}, Actual: {inserted_count}')
        
        result_data = {
            'date': month_str,
            'rows_inserted': inserted_count,
            'zip_checksum': actual_checksum,
            'csv_checksum': csv_checksum,
            'data_verification': data_verification
        }
        
        context.log.info(f"Successfully processed {month_str}")
        return result_data
        
    except Exception as e:
        raise e
    finally:
        # Ensure client is disconnected and resources are cleaned up
        if client:
            try:
                client.disconnect()
            except:
                pass
        # Clear large variables to help garbage collection
        data = None
