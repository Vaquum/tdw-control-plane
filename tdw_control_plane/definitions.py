from dagster import Definitions, define_asset_job, schedule

# Import the assets
from .assets.daily_trades_to_clickhouse import daily_trades_to_clickhouse
from .assets.monthly_trades_to_clickhouse import monthly_trades_to_clickhouse
from .assets.create_tdw_database import create_tdw_database
from .assets.create_binance_trades_table import create_binance_trades_table

# Define the setup job for database creation
setup_job = define_asset_job(
    name="tdw_setup",
    selection=["create_tdw_database", "create_binance_trades_table"]
)

# Define the daily pipeline job
daily_pipeline_job = define_asset_job(
    name="daily_binance_pipeline",
    selection=["daily_trades_to_clickhouse"]
)

# Define the monthly pipeline job for initial data loading
monthly_pipeline_job = define_asset_job(
    name="monthly_binance_pipeline",
    selection=["monthly_trades_to_clickhouse"]
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
    assets=[daily_trades_to_clickhouse, monthly_trades_to_clickhouse, create_tdw_database, create_binance_trades_table],
    schedules=[daily_pipeline_schedule],
    jobs=[setup_job, daily_pipeline_job, monthly_pipeline_job]
)
