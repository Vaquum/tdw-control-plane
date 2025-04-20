from dagster import Definitions, define_asset_job, schedule

# Import the single asset that handles everything
from .assets.daily_trades_to_clickhouse import daily_trades_to_clickhouse

# Define the daily pipeline job
daily_pipeline_job = define_asset_job(
    name="daily_binance_pipeline",
    selection=["daily_trades_to_clickhouse"]
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
    assets=[daily_trades_to_clickhouse],
    schedules=[daily_pipeline_schedule],
    jobs=[daily_pipeline_job]
)
