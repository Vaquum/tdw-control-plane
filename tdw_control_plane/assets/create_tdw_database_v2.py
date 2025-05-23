from .utils.clickhouse_client import get_clickhouse_client
from dagster import asset

CLICKHOUSE_DATABASE = 'tdw'
client = get_clickhouse_client()

@asset(group_name=f'create_db_{CLICKHOUSE_DATABASE}',
       description=f'Creates the database {CLICKHOUSE_DATABASE}')

def create_database(context):

    res = client.query(f"SHOW DATABASES LIKE '{CLICKHOUSE_DATABASE}'")
    
    if not res.result_set:
        client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE} ENGINE = Atomic")
        context.log.info(f"Created database {CLICKHOUSE_DATABASE}.")

    else:
        context.log.info(f"Database {CLICKHOUSE_DATABASE} already exist, did nothing.")
