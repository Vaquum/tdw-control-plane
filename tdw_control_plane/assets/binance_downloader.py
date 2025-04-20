import os
import zipfile
from dagster import asset, DailyPartitionsDefinition

# Use the same daily partition definition
daily_partitions = DailyPartitionsDefinition(
    start_date="2017-08-17"
)

@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Extracts data from the downloaded zip file",
    deps=["binance_btc_trades_file"]
)
def extract_binance_trades(context, binance_btc_trades_file):
    """
    Extracts trade data from the downloaded zip file.
    """
    zip_path = binance_btc_trades_file["path"]
    
    # Get the base storage directory from environment
    local_storage_dir = os.environ.get("LOCAL_STORAGE_DIR", "/opt/binance_data")
    
    # Create directory for extracted files if it doesn't exist
    extract_dir = os.path.join(local_storage_dir, "extracted_zips")
    os.makedirs(extract_dir, exist_ok=True)
    
    try:
        # Extract the zip file
        context.log.info(f"Extracting {zip_path}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get the CSV filename inside the zip
            csv_filename = zip_ref.namelist()[0]
            
            # Extract to the directory
            zip_ref.extractall(extract_dir)
            
            extracted_path = os.path.join(extract_dir, csv_filename)
            context.log.info(f"Successfully extracted to {extracted_path}")
            
            return {
                "csv_path": extracted_path,
                "date": binance_btc_trades_file["date"]
            }
    
    except Exception as e:
        context.log.error(f"Error extracting {zip_path}: {e}")
        raise e
