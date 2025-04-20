import os
import requests
from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition


# Create a daily partition definition starting from when data is available
daily_partitions = DailyPartitionsDefinition(
    start_date="2017-08-17"  # Starting from the earliest date in your original code
)


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Downloads Binance BTC trade data files",
    compute_kind="python",
    io_manager_key="io_manager"
)
def binance_btc_trades_file(context: AssetExecutionContext):
    """
    Downloads Binance BTC trade data file for a specific date.
    """
    # Create directory for zip files if it doesn't exist
    os.makedirs("zips", exist_ok=True)
    
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
    
    # Construct the filename and URL
    filename = f"BTCUSDT-trades-{date_str}.zip"
    file_url = base_url + filename
    
    # Path to save the zip file
    zip_path = os.path.join("zips", filename)
    
    # Log the start of download
    context.log.info(f"Downloading {filename} from {file_url}")
    
    try:
        # Download the file
        response = requests.get(file_url)
        response.raise_for_status()
        
        # Save the file
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        context.log.info(f"Successfully downloaded {filename}")
        return {"filename": filename, "status": "downloaded", "path": zip_path}
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            msg = f"File not found: {filename} (Status code: {e.response.status_code})"
            context.log.error(msg)
            raise FileNotFoundError(msg)
        else:
            context.log.error(f"HTTP error occurred: {e}")
            raise e
    except Exception as e:
        context.log.error(f"Error downloading {filename}: {e}")
        raise e
