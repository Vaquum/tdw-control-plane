import os
from clickhouse_connect import get_client

def get_clickhouse_client():

    '''Returns a ClickHouse client.'''

    CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
    CLICKHOUSE_PORT = int(os.environ.get('CLICKHOUSE_PORT', 8123))
    CLICKHOUSE_USER = os.environ.get('CLICKHOUSE_USER', 'default')
    
    client = get_client(host=CLICKHOUSE_HOST,
                        port=CLICKHOUSE_PORT,
                        username=CLICKHOUSE_USER,
                        password=os.environ.get('CLICKHOUSE_PASSWORD'))

    return client
