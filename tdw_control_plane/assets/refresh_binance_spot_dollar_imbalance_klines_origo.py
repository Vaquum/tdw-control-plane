from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import sqrt

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
TRADE_FETCH_BATCH_SIZE = 100_000
KLINE_INSERT_BATCH_SIZE = 1_000


@dataclass(frozen=True)
class _Trade:
    trade_id: int
    price: float
    quantity: float
    quote_quantity: float
    is_buyer_maker: int
    traded_at: datetime


@dataclass
class _DollarImbalanceBar:
    dollar_imbalance_bar_id: int
    start_datetime: datetime
    end_datetime: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    mean_price: float
    price_m2: float
    prices: list[float]
    volume: float
    maker_count: float
    no_of_trades: int
    open_liquidity: float
    high_liquidity: float
    low_liquidity: float
    close_liquidity: float
    liquidity_sum: float
    maker_volume: float
    maker_liquidity: float
    taker_buy_liquidity: float
    taker_sell_liquidity: float

    @classmethod
    def from_trade(cls, dollar_imbalance_bar_id: int, trade: _Trade) -> '_DollarImbalanceBar':
        liquidity = trade.price * trade.quantity
        maker_volume = trade.quantity if trade.is_buyer_maker == 1 else 0.0
        maker_liquidity = liquidity if trade.is_buyer_maker == 1 else 0.0
        taker_buy_liquidity = trade.quote_quantity if trade.is_buyer_maker == 0 else 0.0
        taker_sell_liquidity = trade.quote_quantity if trade.is_buyer_maker == 1 else 0.0
        return cls(
            dollar_imbalance_bar_id=dollar_imbalance_bar_id,
            start_datetime=trade.traded_at,
            end_datetime=trade.traded_at,
            open_price=trade.price,
            high_price=trade.price,
            low_price=trade.price,
            close_price=trade.price,
            mean_price=trade.price,
            price_m2=0.0,
            prices=[trade.price],
            volume=trade.quantity,
            maker_count=float(trade.is_buyer_maker),
            no_of_trades=1,
            open_liquidity=liquidity,
            high_liquidity=liquidity,
            low_liquidity=liquidity,
            close_liquidity=liquidity,
            liquidity_sum=liquidity,
            maker_volume=maker_volume,
            maker_liquidity=maker_liquidity,
            taker_buy_liquidity=taker_buy_liquidity,
            taker_sell_liquidity=taker_sell_liquidity,
        )

    def add_trade(self, trade: _Trade) -> None:
        liquidity = trade.price * trade.quantity
        self.end_datetime = trade.traded_at
        self.high_price = max(self.high_price, trade.price)
        self.low_price = min(self.low_price, trade.price)
        self.close_price = trade.price

        self.no_of_trades += 1
        price_delta = trade.price - self.mean_price
        self.mean_price += price_delta / self.no_of_trades
        self.price_m2 += price_delta * (trade.price - self.mean_price)
        self.prices.append(trade.price)

        self.volume += trade.quantity
        self.maker_count += trade.is_buyer_maker
        self.high_liquidity = max(self.high_liquidity, liquidity)
        self.low_liquidity = min(self.low_liquidity, liquidity)
        self.close_liquidity = liquidity
        self.liquidity_sum += liquidity

        if trade.is_buyer_maker == 1:
            self.maker_volume += trade.quantity
            self.maker_liquidity += liquidity
            self.taker_sell_liquidity += trade.quote_quantity
        else:
            self.taker_buy_liquidity += trade.quote_quantity

    @property
    def dollar_imbalance(self) -> float:
        return self.taker_buy_liquidity - self.taker_sell_liquidity

    def to_insert_row(self) -> tuple[object, ...]:
        sorted_prices = sorted(self.prices)
        return (
            self.start_datetime,
            self.end_datetime,
            self.dollar_imbalance_bar_id,
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.mean_price,
            sqrt(self.price_m2 / self.no_of_trades),
            _quantile_exact(sorted_prices, 0.5),
            _quantile_exact(sorted_prices, 0.75) - _quantile_exact(sorted_prices, 0.25),
            self.volume,
            self.maker_count / self.no_of_trades,
            self.no_of_trades,
            self.open_liquidity,
            self.high_liquidity,
            self.low_liquidity,
            self.close_liquidity,
            self.liquidity_sum,
            self.maker_volume,
            self.maker_liquidity,
            self.taker_buy_liquidity,
            self.taker_sell_liquidity,
            self.dollar_imbalance,
        )


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


def _clickhouse_datetime64(value: datetime) -> str:
    return value.strftime('%Y-%m-%d %H:%M:%S.%f')


def _partition_datetime_bounds(partition_date: str) -> tuple[str, str]:
    partition_start = datetime.strptime(partition_date, '%Y-%m-%d')
    partition_end = partition_start + timedelta(days=1)
    return _clickhouse_datetime64(partition_start), _clickhouse_datetime64(partition_end)


def _quantile_exact(sorted_values: list[float], level: float) -> float:
    index = int(level * len(sorted_values))
    if index == len(sorted_values):
        index -= 1
    return sorted_values[index]


def _parse_trade_row(row: tuple[object, ...]) -> _Trade:
    if len(row) != 6:
        raise TypeError(f'Expected 6 trade columns from ClickHouse, got {len(row)}')

    trade_id, price, quantity, quote_quantity, is_buyer_maker, traded_at = row
    if not isinstance(trade_id, int):
        raise TypeError(f'Expected int trade_id from ClickHouse, got {type(trade_id).__name__}')
    if not isinstance(price, float):
        raise TypeError(f'Expected float price from ClickHouse, got {type(price).__name__}')
    if not isinstance(quantity, float):
        raise TypeError(f'Expected float quantity from ClickHouse, got {type(quantity).__name__}')
    if not isinstance(quote_quantity, float):
        raise TypeError(
            f'Expected float quote_quantity from ClickHouse, got {type(quote_quantity).__name__}'
        )
    if not isinstance(is_buyer_maker, int):
        raise TypeError(
            'Expected int is_buyer_maker from ClickHouse, '
            f'got {type(is_buyer_maker).__name__}'
        )
    if not isinstance(traded_at, datetime):
        raise TypeError(f'Expected datetime from ClickHouse, got {type(traded_at).__name__}')

    return _Trade(
        trade_id=trade_id,
        price=price,
        quantity=quantity,
        quote_quantity=quote_quantity,
        is_buyer_maker=is_buyer_maker,
        traded_at=traded_at,
    )


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


def _fetch_trade_batch(
    client: ClickHouseClientProtocol,
    database: str,
    partition_start: str,
    partition_end: str,
    last_trade: _Trade | None,
) -> list[tuple[object, ...]]:
    cursor_clause = ''
    if last_trade is not None:
        cursor_clause = (
            f"AND (datetime > toDateTime64('{_clickhouse_datetime64(last_trade.traded_at)}', 6) "
            "OR ("
            f"datetime = toDateTime64('{_clickhouse_datetime64(last_trade.traded_at)}', 6) "
            f"AND trade_id > {last_trade.trade_id}"
            '))'
        )

    return client.execute(
        f"""
        SELECT
            trade_id,
            price,
            quantity,
            quote_quantity,
            is_buyer_maker,
            datetime
        FROM {database}.{RAW_TABLE_NAME}
        WHERE datetime >= toDateTime64('{partition_start}', 6)
          AND datetime < toDateTime64('{partition_end}', 6)
          {cursor_clause}
        ORDER BY datetime, trade_id
        LIMIT {TRADE_FETCH_BATCH_SIZE}
        """
    )


def _insert_kline_rows(
    client: ClickHouseClientProtocol,
    database: str,
    rows: list[tuple[object, ...]],
) -> None:
    client.execute(
        f'INSERT INTO {database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME} VALUES',
        rows,
    )


def _flush_kline_rows(
    client: ClickHouseClientProtocol,
    database: str,
    rows: list[tuple[object, ...]],
) -> None:
    if rows:
        _insert_kline_rows(client, database, rows)
        rows.clear()


def _insert_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    partition_start, partition_end = _partition_datetime_bounds(partition_date)
    pending_rows: list[tuple[object, ...]] = []
    last_trade: _Trade | None = None
    open_bar: _DollarImbalanceBar | None = None
    next_bar_id = 0

    batch = _fetch_trade_batch(
        client,
        database,
        partition_start,
        partition_end,
        last_trade,
    )
    while batch:
        for row in batch:
            trade = _parse_trade_row(row)
            if open_bar is None:
                open_bar = _DollarImbalanceBar.from_trade(next_bar_id, trade)
            else:
                open_bar.add_trade(trade)

            if abs(open_bar.dollar_imbalance) >= DOLLAR_IMBALANCE_KLINE_SIZE:
                pending_rows.append(open_bar.to_insert_row())
                next_bar_id += 1
                open_bar = None

            last_trade = trade
            if len(pending_rows) >= KLINE_INSERT_BATCH_SIZE:
                _flush_kline_rows(client, database, pending_rows)

        batch = _fetch_trade_batch(
            client,
            database,
            partition_start,
            partition_end,
            last_trade,
        )

    if open_bar is not None:
        pending_rows.append(open_bar.to_insert_row())

    _flush_kline_rows(client, database, pending_rows)


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
