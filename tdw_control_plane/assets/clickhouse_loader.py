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
CLICKHOUSE_TABLE = "btc_trades"

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
        
        # Make sure the table exists (create if not exists)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} (
            trade_id UInt64,
            price Float64,
            quantity Float64,
            quote_quantity Float64,
            timestamp UInt64,
            is_buyer_maker Bool,
            is_best_match Bool,
            datetime DateTime64(3, 'UTC')
        ) ENGINE = MergeTree()
        ORDER BY (datetime, trade_id);
        """
        
        context.log.info("Ensuring table exists in Clickhouse")
        client.execute(create_table_sql)
        
        # Process the CSV file
        total_rows = 0
        batch_size = 100000  # Process in batches to avoid memory issues
        
        context.log.info(f"Processing CSV file: {csv_path}")
        
        with open(csv_path, 'r') as f:
            # Read the header to get column names
            reader = csv.reader(f)
            headers = next(reader)
            
            # Map column names to our schema
            # The CSV format from Binance has these columns: id,price,qty,quoteQty,time,isBuyerMaker,isBestMatch
            
            # Prepare to process the file
            batch = []
            
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
                dt = datetime.fromtimestamp(timestamp / 1000.0)
                
                # Add to batch
                batch.append((
                    trade_id,
                    price,
                    quantity,
                    quote_quantity,
                    timestamp,
                    is_buyer_maker,
                    is_best_match,
                    dt
                ))
                
                # Insert when batch is full
                if len(batch) >= batch_size:
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
                        batch
                    )
                    
                    total_rows += len(batch)
                    context.log.info(f"Inserted {len(batch)} rows, total: {total_rows}")
                    batch = []
            
            # Insert remaining rows
            if batch:
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
                    batch
                )
                
                total_rows += len(batch)
                context.log.info(f"Inserted {len(batch)} rows, total: {total_rows}")
        
        # Also populate the for_framing_without_time table
        create_framing_table_sql = """
        CREATE TABLE IF NOT EXISTS trades.for_framing_without_time (
            trade_id UInt64,
            price Float64,
            quantity Float64,
            quote_quantity Float64,
            is_buyer_maker Bool
        ) ENGINE = MergeTree()
        PRIMARY KEY (trade_id);
        """
        
        client.execute(create_framing_table_sql)
        
        # Copy data to the framing table
        copy_to_framing_sql = f"""
        INSERT INTO trades.for_framing_without_time
        SELECT
            trade_id,
            price,
            quantity,
            quote_quantity,
            is_buyer_maker
        FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
        WHERE toDate(datetime) = toDate('{date_str}')
        AND NOT EXISTS (
            SELECT 1 FROM trades.for_framing_without_time t 
            WHERE t.trade_id = {CLICKHOUSE_TABLE}.trade_id
        )
        """
        
        client.execute(copy_to_framing_sql)
        context.log.info(f"Data also copied to for_framing_without_time table")
        
        return {
            "date": date_str,
            "rows_inserted": total_rows
        }
    
    except Exception as e:
        context.log.error(f"Error loading data to Clickhouse: {e}")
        raise e
