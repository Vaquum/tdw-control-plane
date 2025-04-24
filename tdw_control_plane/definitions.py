from dagster import Definitions, define_asset_job, schedule

from .assets.daily_trades_to_tdw import insert_daily_binance_trades_to_tdw
from .assets.monthly_trades_to_tdw import insert_monthly_binance_trades_to_tdw
from .assets.create_tdw_database import create_tdw_database
from .assets.create_binance_trades_table import create_binance_trades_table

create_tdw_database_job = define_asset_job(
    name="create_tdw_database_job",
    selection=["create_tdw_database"]
)

create_binance_trades_table_job = define_asset_job(
    name="create_binance_trades_table_job",
    selection=["create_binance_trades_table"]
)

insert_monthly_binance_trades_job = define_asset_job(
    name="insert_monthly_trades_to_tdw_job",
    selection=["insert_monthly_binance_trades_to_tdw"]
)

insert_daily_binance_trades_job = define_asset_job(
    name="insert_daily_trades_to_tdw_job",
    selection=["insert_daily_binance_trades_to_tdw"]
)

@schedule(
    job=insert_daily_binance_trades_job,
    cron_schedule="0 1 * * *",
    execution_timezone="UTC"
)
def daily_pipeline_schedule():
    return {}

defs = Definitions(
    assets=[create_tdw_database,
            create_binance_trades_table,
            insert_monthly_binance_trades_to_tdw,
            insert_daily_binance_trades_to_tdw],
    
    schedules=[daily_pipeline_schedule],
    
    jobs=[create_tdw_database_job,
          create_binance_trades_table_job,
          insert_monthly_binance_trades_job,
          insert_daily_binance_trades_job]
)
