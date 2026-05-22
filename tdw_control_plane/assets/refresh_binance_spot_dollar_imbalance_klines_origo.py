import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Protocol, cast

import numpy as np
import numpy.typing as npt
import pyarrow as pa
from dagster import AssetExecutionContext, AssetRecordsFilter, asset

from .create_binance_spot_dollar_imbalance_klines_table_origo import (
    DOLLAR_IMBALANCE_KLINES_TABLE_NAME,
    create_binance_spot_dollar_imbalance_klines_table_origo,
)
from .create_binance_trades_table_origo import LEDGER_TABLE_NAME, RAW_TABLE_NAME
from .create_origo_database import (
    ClickHouseClientProtocol,
    ClickHouseSettings,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .daily_trades_to_origo import daily_partitions, insert_daily_binance_spot_trades_to_origo

DOLLAR_IMBALANCE_KLINE_SIZE = 100_000.0
DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP = 512
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
RAW_TRADES_ASSET_KEY = insert_daily_binance_spot_trades_to_origo.key


class _ClickHouseArrowClientProtocol(Protocol):
    def query_arrow(self, query: str) -> pa.Table:
        raise NotImplementedError

    def insert_arrow(self, table: str, arrow_table: pa.Table) -> object:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


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


def _get_clickhouse_http_port() -> int:
    value = os.environ.get('CLICKHOUSE_HTTP_PORT', str(DEFAULT_CLICKHOUSE_HTTP_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError('CLICKHOUSE_HTTP_PORT environment variable must be an integer.') from exc


def _make_clickhouse_arrow_client(
    settings: ClickHouseSettings,
) -> _ClickHouseArrowClientProtocol:
    client_factory = getattr(import_module('clickhouse_connect'), 'get_client')
    return cast(
        _ClickHouseArrowClientProtocol,
        client_factory(
            host=settings.host,
            port=_get_clickhouse_http_port(),
            username=settings.user,
            password=settings.password,
            database=settings.database,
        ),
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


def _count_raw_partition_rows(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> int:
    partition_start, partition_end = _partition_datetime_bounds(partition_date)
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{RAW_TABLE_NAME}
        WHERE datetime >= toDateTime64('{partition_start}', 6)
          AND datetime < toDateTime64('{partition_end}', 6)
        """
    )
    return int(result[0][0])


def _raw_ledger_inserted_row_count(
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> int | None:
    result = client.execute(
        f"""
        SELECT inserted_row_count
        FROM {database}.{LEDGER_TABLE_NAME}
        WHERE source_date = toDate('{partition_date}')
          AND source_file = 'BTCUSDT-trades-{partition_date}.zip'
          AND status = 'success'
        ORDER BY loaded_at DESC
        LIMIT 1
        """
    )
    if len(result) == 0:
        return None
    return int(result[0][0])


def _raw_partition_was_materialized(
    context: AssetExecutionContext,
    partition_date: str,
) -> bool:
    records = context.instance.fetch_materializations(
        AssetRecordsFilter(
            asset_key=RAW_TRADES_ASSET_KEY,
            asset_partitions=[partition_date],
        ),
        limit=1,
    )
    return len(records.records) == 1


def _ensure_raw_partition_ready(
    context: AssetExecutionContext,
    client: ClickHouseClientProtocol,
    database: str,
    partition_date: str,
) -> None:
    if not _raw_partition_was_materialized(context, partition_date):
        raise RuntimeError(
            f'Raw Binance spot trades have no Dagster materialization for {partition_date}. '
            f'Run insert_daily_binance_spot_trades_to_origo for that partition first.'
        )

    raw_count = _count_raw_partition_rows(client, database, partition_date)
    if raw_count == 0:
        raise RuntimeError(
            f'Raw Binance spot trades are missing for {partition_date}. '
            f'Run insert_daily_binance_spot_trades_to_origo for that partition first.'
        )

    ledger_count = _raw_ledger_inserted_row_count(client, database, partition_date)
    if ledger_count is None:
        raise RuntimeError(
            f'Raw Binance spot trades ingestion ledger is missing for {partition_date}. '
            f'Run insert_daily_binance_spot_trades_to_origo for that partition first.'
        )

    if raw_count != ledger_count:
        raise RuntimeError(
            f'Raw Binance spot trades row count mismatch for {partition_date}: '
            f'raw={raw_count}, ledger={ledger_count}.'
        )


def _query_partition_trades(
    client: _ClickHouseArrowClientProtocol,
    database: str,
    partition_start: str,
    partition_end: str,
) -> pa.Table:
    return client.query_arrow(
        f"""
        SELECT
            price,
            quantity,
            quote_quantity,
            is_buyer_maker,
            datetime
        FROM {database}.{RAW_TABLE_NAME}
        WHERE datetime >= toDateTime64('{partition_start}', 6)
          AND datetime < toDateTime64('{partition_end}', 6)
        ORDER BY datetime, trade_id
        """
    )


def _float64_column(table: pa.Table, column_name: str) -> npt.NDArray[np.float64]:
    return np.asarray(table.column(column_name), dtype=np.float64)


def _uint8_column(table: pa.Table, column_name: str) -> npt.NDArray[np.uint8]:
    return np.asarray(table.column(column_name), dtype=np.uint8)


def _datetime_column(table: pa.Table, column_name: str) -> npt.NDArray[np.datetime64]:
    return cast(npt.NDArray[np.datetime64], np.asarray(table.column(column_name)))


def _bar_boundaries(
    quote_quantities: npt.NDArray[np.float64],
    maker_flags: npt.NDArray[np.uint8],
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    signed_quotes = quote_quantities.copy()
    signed_quotes[maker_flags == 1] *= -1.0
    cumulative_quotes = np.cumsum(signed_quotes, out=signed_quotes)

    starts: list[int] = []
    ends: list[int] = []
    start_index = 0
    row_count = len(cumulative_quotes)

    while start_index < row_count:
        base_quote = cumulative_quotes[start_index - 1] if start_index > 0 else 0.0
        end_index = min(start_index + DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP, row_count)

        while True:
            window = np.abs(cumulative_quotes[start_index:end_index] - base_quote)
            hits = np.flatnonzero(window >= DOLLAR_IMBALANCE_KLINE_SIZE)
            if len(hits) > 0:
                starts.append(start_index)
                ends.append(start_index + int(hits[0]))
                break

            if end_index == row_count:
                starts.append(start_index)
                ends.append(row_count - 1)
                break

            end_index = min(end_index + DOLLAR_IMBALANCE_BOUNDARY_SEARCH_STEP, row_count)

        start_index = ends[-1] + 1

    return np.array(starts, dtype=np.int64), np.array(ends, dtype=np.int64)


def _quantile_index(length: int, level: float) -> int:
    index = int(level * length)
    if index == length:
        index -= 1
    return index


def _price_distribution_columns(
    prices: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
    ends: npt.NDArray[np.int64],
) -> tuple[npt.NDArray[np.float64], npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    stds: list[float] = []
    medians: list[float] = []
    iqrs: list[float] = []

    for start, end in zip(starts, ends, strict=True):
        bar_prices = prices[start : end + 1]
        row_count = len(bar_prices)
        q25_index = _quantile_index(row_count, 0.25)
        median_index = _quantile_index(row_count, 0.5)
        q75_index = _quantile_index(row_count, 0.75)
        partitioned_prices = np.partition(bar_prices, (q25_index, median_index, q75_index))

        stds.append(float(bar_prices.std()))
        medians.append(float(partitioned_prices[median_index]))
        iqrs.append(float(partitioned_prices[q75_index] - partitioned_prices[q25_index]))

    return (
        np.array(stds, dtype=np.float64),
        np.array(medians, dtype=np.float64),
        np.array(iqrs, dtype=np.float64),
    )


def _sum_by_bar(
    values: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
) -> npt.NDArray[np.float64]:
    return cast(npt.NDArray[np.float64], np.add.reduceat(values, starts))


def _max_by_bar(
    values: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
) -> npt.NDArray[np.float64]:
    return cast(npt.NDArray[np.float64], np.maximum.reduceat(values, starts))


def _min_by_bar(
    values: npt.NDArray[np.float64],
    starts: npt.NDArray[np.int64],
) -> npt.NDArray[np.float64]:
    return cast(npt.NDArray[np.float64], np.minimum.reduceat(values, starts))


def _arrow_array(values: Sequence[object] | npt.NDArray[np.generic]) -> pa.Array:
    return pa.array(values)


def _kline_rows(table: pa.Table) -> pa.Table:
    prices = _float64_column(table, 'price')
    quantities = _float64_column(table, 'quantity')
    quote_quantities = _float64_column(table, 'quote_quantity')
    maker_flags = _uint8_column(table, 'is_buyer_maker')
    datetimes = _datetime_column(table, 'datetime')

    starts, ends = _bar_boundaries(quote_quantities, maker_flags)
    lengths = (ends - starts + 1).astype(np.uint64)

    liquidities = prices * quantities
    taker_buy_quotes = quote_quantities.copy()
    taker_buy_quotes[maker_flags == 1] = 0.0
    taker_buy_liquidity = _sum_by_bar(taker_buy_quotes, starts)
    del taker_buy_quotes

    taker_sell_quotes = quote_quantities.copy()
    taker_sell_quotes[maker_flags == 0] = 0.0
    taker_sell_liquidity = _sum_by_bar(taker_sell_quotes, starts)
    del taker_sell_quotes

    maker_float = maker_flags.astype(np.float64)
    maker_volume = _sum_by_bar(maker_float * quantities, starts)
    maker_liquidity = _sum_by_bar(maker_float * liquidities, starts)
    stds, medians, iqrs = _price_distribution_columns(prices, starts, ends)

    return pa.table(
        {
            'start_datetime': _arrow_array(datetimes[starts]),
            'end_datetime': _arrow_array(datetimes[ends]),
            'dollar_imbalance_bar_id': _arrow_array(
                np.arange(len(starts), dtype=np.uint64)
            ),
            'open': _arrow_array(prices[starts]),
            'high': _arrow_array(_max_by_bar(prices, starts)),
            'low': _arrow_array(_min_by_bar(prices, starts)),
            'close': _arrow_array(prices[ends]),
            'mean': _arrow_array(_sum_by_bar(prices, starts) / lengths),
            'std': _arrow_array(stds),
            'median': _arrow_array(medians),
            'iqr': _arrow_array(iqrs),
            'volume': _arrow_array(_sum_by_bar(quantities, starts)),
            'maker_ratio': _arrow_array(_sum_by_bar(maker_float, starts) / lengths),
            'no_of_trades': _arrow_array(lengths),
            'open_liquidity': _arrow_array(liquidities[starts]),
            'high_liquidity': _arrow_array(_max_by_bar(liquidities, starts)),
            'low_liquidity': _arrow_array(_min_by_bar(liquidities, starts)),
            'close_liquidity': _arrow_array(liquidities[ends]),
            'liquidity_sum': _arrow_array(_sum_by_bar(liquidities, starts)),
            'maker_volume': _arrow_array(maker_volume),
            'maker_liquidity': _arrow_array(maker_liquidity),
            'taker_buy_liquidity': _arrow_array(taker_buy_liquidity),
            'taker_sell_liquidity': _arrow_array(taker_sell_liquidity),
            'dollar_imbalance': _arrow_array(taker_buy_liquidity - taker_sell_liquidity),
        }
    )


def _insert_partition_rows(
    settings: ClickHouseSettings,
    partition_date: str,
) -> None:
    partition_start, partition_end = _partition_datetime_bounds(partition_date)
    client = _make_clickhouse_arrow_client(settings)

    try:
        trade_rows = _query_partition_trades(
            client,
            settings.database,
            partition_start,
            partition_end,
        )
        if trade_rows.num_rows == 0:
            return

        client.insert_arrow(
            f'{settings.database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}',
            _kline_rows(trade_rows),
        )
    finally:
        client.close()


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
        _ensure_raw_partition_ready(context, client, settings.database, partition_date)

        existing_count = _count_partition_rows(client, settings.database, partition_date)
        if existing_count > 0:
            context.log.info(
                f'Found {existing_count} existing Binance spot dollar imbalance kline rows for '
                f'{partition_date}. Replacing that partition.'
            )
            _delete_partition_rows(client, settings.database, partition_date)

        _insert_partition_rows(settings, partition_date)
        inserted_count = _count_partition_rows(client, settings.database, partition_date)

        return {
            'status': 'success',
            'partition_date': partition_date,
            'rows_inserted': inserted_count,
            'table': f'{settings.database}.{DOLLAR_IMBALANCE_KLINES_TABLE_NAME}',
        }
    finally:
        client.disconnect()
