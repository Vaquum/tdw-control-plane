from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster.core.storage.fs_io_manager import fs_io_manager
from .assets.binance_downloader import binance_btc_trades_file


# Define a job that will download the previous day's data
daily_download_job = define_asset_job(
    name="daily_binance_download",
    selection="binance_btc_trades_file"
)


# Schedule to run daily at 1:00 AM UTC
daily_download_schedule = ScheduleDefinition(
    name="daily_binance_download_schedule",
    cron_schedule="0 1 * * *",
    job=daily_download_job,
    execution_timezone="UTC"
)


# Create the Dagster definitions
defs = Definitions(
    assets=[binance_btc_trades_file],
    schedules=[daily_download_schedule],
    jobs=[daily_download_job],
    resources={
        "io_manager": fs_io_manager
    }
)
