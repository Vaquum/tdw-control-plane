import os
import csv
from datetime import datetime
from clickhouse_driver import Client as ClickhouseClient
from dagster import asset, DailyPartitionsDefinition

# Use the same daily partition definition
daily_partitions = DailyPartitionsDefinition(
    start_date="2017-08-17"
)

# Configure Clickhouse connection
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = "trades"
CLICKHOUSE_TABLE = "daily_updates"

@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Loads extracted data into Clickhouse",
    deps=["extract_binance_trades"]
)
def load_to_clickhouse(context, extract_binance_trades):
    """
    Loads the extracted trade data into Clickhouse.
    """
    csv_path = extract_binance_trades["csv_path"]
    date_str = extract_binance_trades["date"]
    
    try:
        # Connect to Clickhouse
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        
        context.log.info(f"Processing CSV file: {csv_path}")
        
        # Process the whole file at once
        data = []
        
        with open(csv_path, 'r') as f:
            # Read the header to get column names
            reader = csv.reader(f)
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
        
        # Insert all data at once
        context.log.info(f"Inserting {len(data)} rows into Clickhouse")
        
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
        
        context.log.info(f"Successfully inserted {len(data)} rows into {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}")
        
        return {
            "date": date_str,
            "rows_inserted": len(data)
        }
    
    except Exception as e:
        context.log.error(f"Error loading data to Clickhouse: {e}")
        raise e
