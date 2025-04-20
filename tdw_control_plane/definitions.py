from dagster import Definitions, define_asset_job, schedule

# Import assets
from tdw_control_plane.assets.binance_downloader import binance_btc_trades_file
from tdw_control_plane.assets.binance_extractor import extract_binance_trades
from tdw_control_plane.assets.clickhouse_loader import load_to_clickhouse

# Define the daily pipeline job
daily_pipeline_job = define_asset_job(
    name="daily_binance_pipeline",
    selection=[
        "binance_btc_trades_file", 
        "extract_binance_trades", 
        "load_to_clickhouse"
    ]
)

# Schedule to run daily at 1:00 AM UTC
@schedule(
    job=daily_pipeline_job,
    cron_schedule="0 1 * * *",
    execution_timezone="UTC"
)
def daily_pipeline_schedule():
    return {}

# Create the Dagster definitions
defs = Definitions(
    assets=[
        binance_btc_trades_file, 
        extract_binance_trades, 
        load_to_clickhouse
    ],
    schedules=[daily_pipeline_schedule],
    jobs=[daily_pipeline_job]
)
