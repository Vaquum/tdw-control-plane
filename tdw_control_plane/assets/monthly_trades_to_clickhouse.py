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

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', '37.27.112.187')
CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'tdw')
CLICKHOUSE_TABLE = os.environ.get('CLICKHOUSE_TABLE', 'binance_trades')

monthly_partitions = MonthlyPartitionsDefinition(
    start_date='2019-01-01'
)


@asset(
    partitions_def=monthly_partitions,
    group_name='binance_data',
    description='Downloads, validates, extracts, and loads Binance BTC trade data into Clickhouse'
)

def monthly_trades_to_clickhouse(context):
    # For initial database build, process all months from 2019 to present
    # Generate a list of all months to process
    month_strings = _generate_month_strings()
    results = []
    
    for month_str in month_strings:
        result = _process_month(month_str)
        results.append(result)
        
    return results

def _process_month(month_str):
    base_url = 'https://data.binance.vision/data/spot/monthly/trades/BTCUSDT/'
    file_url = base_url + month_str
    checksum_url = file_url + '.CHECKSUM'
    
    # Download and verify checksum
    checksum_response = requests.get(checksum_url)
    checksum_response.raise_for_status()
    
    expected_checksum = checksum_response.text.split()[-1].strip()

    response = requests.get(file_url)
    response.raise_for_status()
    zip_data = response.content
    
    actual_checksum = hashlib.sha256(zip_data).hexdigest()
    if actual_checksum != expected_checksum:
        raise ValueError(f'Checksum mismatch! Expected: {expected_checksum}, Actual: {actual_checksum}')
    
    csv_content = None
    csv_filename = None
    
    # Extract CSV from zip file
    with zipfile.ZipFile(BytesIO(zip_data)) as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        
        with zip_ref.open(csv_filename) as csv_file:
            csv_content = csv_file.read()
    
    # Calculate CSV checksum
    csv_checksum = hashlib.sha256(csv_content).hexdigest()
    
    # Parse CSV data
    data = []
    
    csv_text = csv_content.decode('utf-8')
    reader = csv.reader(csv_text.splitlines())
    headers = next(reader)
    
    for row in reader:
        trade_id = int(row[0])
        price = float(row[1])
        quantity = float(row[2])
        quote_quantity = float(row[3])
        timestamp = int(row[4])
        is_buyer_maker = row[5].lower() == 'true'
        is_best_match = row[6].lower() == 'true'
        
        # Binance started with milliseconds, then switched to microseconds
        if len(str(timestamp)) == 13:
            dt = datetime.fromtimestamp(timestamp / 1000.0)
        elif len(str(timestamp)) == 16:
            dt = datetime.fromtimestamp(timestamp / 1000000.0)
        
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
    
    # Connect to ClickHouse
    client = None
    try:
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        
        # Insert data
        client.execute(
            f'''
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
            ''',
            data
        )
        
        # Verify insertion
        result = client.execute(f'''
            SELECT count(*)
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE datetime >= toDate('{month_start}')
            AND datetime <  addMonths(toDate('{month_start}'), 1)
        ''')
        inserted_count = result[0][0]

        # Compute a checksum for the inserted month's data
        checksum_result = client.execute(f'''
            SELECT cityHash64(
                groupArray(toString(trade_id)),
                groupArray(toString(price))
            )
            FROM (
                SELECT trade_id, price
                FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
                WHERE datetime >= toDate('{month_start}')
                AND datetime <  addMonths(toDate('{month_start}'), 1)
                ORDER BY trade_id
            )
        ''')
        data_checksum = checksum_result[0][0]
        
        if inserted_count != len(data):
            raise ValueError(f'Row count mismatch! Expected: {len(data)}, Actual: {inserted_count}')
        
        result_data = {
            'date': month_str,
            'rows_inserted': inserted_count,
            'zip_checksum': actual_checksum,
            'csv_checksum': csv_checksum,
            'data_checksum': data_checksum
        }
        
        print(result_data)
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

def _generate_month_strings(start_year=2019, start_month=1):
    today = date.today()
    y, m = today.year, today.month - 1
    if m == 0: 
        y, m = y - 1, 12
    start_idx = start_year * 12 + start_month - 1
    end_idx = y * 12 + m - 1
    return [
        f'BTCUSDT-trades-{idx//12:04d}-{idx%12+1:02d}.zip'
        for idx in range(start_idx, end_idx + 1)
    ]

def _compute_sha256(data):
    if isinstance(data, bytes):
        return hashlib.sha256(data).hexdigest()
    else:
        sha256_hash = hashlib.sha256()
        for byte_block in iter(lambda: data.read(4096), b""):
            sha256_hash.update(byte_block)
        data.seek(0)
        return sha256_hash.hexdigest()
