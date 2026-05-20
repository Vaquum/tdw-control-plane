from datetime import UTC, datetime, timedelta
from uuid import uuid4

from dagster import AssetExecutionContext, asset

from .create_binance_spot_dollar_imbalance_klines_table_origo import (
    DOLLAR_IMBALANCE_KLINES_TABLE_NAME,
    create_binance_spot_dollar_imbalance_klines_table_origo,
)
from .create_binance_trades_table_origo import RAW_TABLE_NAME
from .create_origo_database import (
    ClickHouseClientProtocol,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .daily_trades_to_origo import daily_partitions, insert_daily_binance_spot_trades_to_origo

DOLLAR_IMBALANCE_KLINE_SIZE = 100_000.0


def _partition_key_or_none(partition_key: object) -> str | None:
    if isinstance(partition_key, str):
        return partition_key
    return None


def _partition_date_from_context(context: AssetExecutionContext) -> str:
    partition_key = _partition_key_or_none(context.partition_key)
    if partition_key is not None:
        return partition_key

    target_date = datetime.now(UTC) - timedelta(days=1)
    return target_date.strftime('%Y-%m-%d')


def _delete_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}
        DELETE WHERE toDate(start_datetime) = toDate('{partition_date}')
        """,
        settings={'mutations_sync': 2},
    )


def _count_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}
        WHERE toDate(start_datetime) = toDate('{partition_date}')
        """
    )
    count_value = result[0][0]
    if not isinstance(count_value, int):
        raise TypeError(f'Expected int scalar from ClickHouse, got {type(count_value).__name__}')
    return count_value


def _fetch_partition_trade_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> list[tuple[object, ...]]:
    return client.execute(
        f"""
        SELECT trade_id, quote_quantity, is_buyer_maker
        FROM {database}.{RAW_TABLE_NAME}
        WHERE toDate(datetime) = toDate('{partition_date}')
        ORDER BY datetime, trade_id
        """
    )


def _parse_trade_row(row: tuple[object, ...]) -> tuple[int, float, int]:
    if len(row) != 3:
        raise TypeError(f'Expected 3 columns from ClickHouse trade row, got {len(row)}')

    trade_id_value, quote_quantity_value, is_buyer_maker_value = row
    if not isinstance(trade_id_value, int):
        raise TypeError(f'Expected int trade_id, got {type(trade_id_value).__name__}')
    if not isinstance(quote_quantity_value, int | float):
        raise TypeError(
            f'Expected numeric quote_quantity, got {type(quote_quantity_value).__name__}'
        )
    if not isinstance(is_buyer_maker_value, int):
        raise TypeError(
            f'Expected int is_buyer_maker, got {type(is_buyer_maker_value).__name__}'
        )

    return trade_id_value, float(quote_quantity_value), is_buyer_maker_value


def _signed_taker_quote(quote_quantity: float, is_buyer_maker: int) -> float:
    if is_buyer_maker == 0:
        return quote_quantity
    if is_buyer_maker == 1:
        return -quote_quantity

    raise ValueError(f'Expected is_buyer_maker to be 0 or 1, got {is_buyer_maker}')


def _trade_bar_assignments(rows: list[tuple[object, ...]]) -> list[tuple[int, int]]:
    assignments: list[tuple[int, int]] = []
    dollar_imbalance_bar_id = 0
    signed_imbalance = 0.0

    for row in rows:
        trade_id, quote_quantity, is_buyer_maker = _parse_trade_row(row)
        signed_imbalance += _signed_taker_quote(quote_quantity, is_buyer_maker)
        assignments.append((trade_id, dollar_imbalance_bar_id))

        if abs(signed_imbalance) >= DOLLAR_IMBALANCE_KLINE_SIZE:
            dollar_imbalance_bar_id += 1
            signed_imbalance = 0.0

    return assignments


def _create_trade_bar_mapping_table(
    client: ClickHouseClientProtocol,
    table_name: str,
) -> None:
    client.execute(
        f"""
        CREATE TEMPORARY TABLE {table_name} (
            trade_id UInt64,
            dollar_imbalance_bar_id UInt64
        )
        ENGINE = Memory
        """
    )


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    assignments = _trade_bar_assignments(
        _fetch_partition_trade_rows(client, database, partition_date)
    )
    if not assignments:
        return

    mapping_table_name = f'dollar_imbalance_trade_bar_ids_{uuid4().hex}'
    _create_trade_bar_mapping_table(client, mapping_table_name)
    client.execute(
        f'INSERT INTO {mapping_table_name} (trade_id, dollar_imbalance_bar_id) VALUES',
        assignments,
    )
    client.execute(
        f"""
        INSERT INTO {database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            dollar_imbalance_bar_id,
            argMin(price, trade_id) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, trade_id) AS close,
            avg(price) AS mean,
            stddevPopStable(price) AS std,
            quantileExact(0.5)(price) AS median,
            quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr,
            sumKahan(quantity) AS volume,
            avg(is_buyer_maker) AS maker_ratio,
            count() AS no_of_trades,
            argMin(price * quantity, trade_id) AS open_liquidity,
            max(price * quantity) AS high_liquidity,
            min(price * quantity) AS low_liquidity,
            argMax(price * quantity, trade_id) AS close_liquidity,
            sum(price * quantity) AS liquidity_sum,
            sumKahan(is_buyer_maker * quantity) AS maker_volume,
            sum(is_buyer_maker * price * quantity) AS maker_liquidity,
            sum(if(is_buyer_maker = 0, price * quantity, 0.0)) AS taker_buy_liquidity,
            sum(if(is_buyer_maker = 1, price * quantity, 0.0)) AS taker_sell_liquidity,
            sum(if(is_buyer_maker = 0, price * quantity, 0.0))
                - sum(if(is_buyer_maker = 1, price * quantity, 0.0)) AS dollar_imbalance
        FROM {database}.{RAW_TABLE_NAME}
        INNER JOIN {mapping_table_name} USING (trade_id)
        WHERE toDate(datetime) = toDate('{partition_date}')
        GROUP BY dollar_imbalance_bar_id
        ORDER BY dollar_imbalance_bar_id
        """
    )


@asset(
    partitions_def=daily_partitions,
    deps=[
        create_binance_spot_dollar_imbalance_klines_table_origo,
        insert_daily_binance_spot_trades_to_origo,
    ],
    group_name='binance_data',
    description=(
        'Refreshes the daily-scoped Binance spot dollar imbalance kline projection '
        'from source-native daily trades; dollar_imbalance_bar_id resets each date '
        'and the final daily bar may be below the dollar imbalance threshold.'
    ),
)
def refresh_binance_spot_dollar_imbalance_klines_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date = _partition_date_from_context(context)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing Binance spot dollar imbalance kline rows for '
                f'{partition_date}. Replacing that partition.'
            )
            _delete_partition_rows(client, settings.database, partition_date)

        _insert_partition_rows(client, settings.database, partition_date)
        inserted_count = _count_partition_rows(client, settings.database, partition_date)

        return {
            'status': 'success',
            'partition_date': partition_date,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
