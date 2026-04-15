import logging
import re
import time

import polars as pl

from tdw_control_plane.utils.get_clickhouse_client import get_clickhouse_client

logger = logging.getLogger(__name__)


def get_binance_spot_klines(
    n_rows: int | None = None,
    kline_size: int = 1,
    start_date_limit: str | None = None,
    end_date_limit: str | None = None,
    show_summary: bool = False,
    table_name: str = "binance_trades_complete",
) -> pl.DataFrame:
    """Query Binance BTCUSDT spot klines from TDW.

    This is TDW's canonical spot-kline query and mirrors the existing
    HistoricalData.get_spot_klines semantics while sourcing data from
    tdw.binance_trades_complete by default.
    """

    if re.fullmatch(r"[A-Za-z0-9_]+", table_name) is None:
        raise ValueError(f"Invalid ClickHouse table name: {table_name}")

    client = get_clickhouse_client()
    try:
        limit = f"LIMIT {n_rows}" if n_rows is not None else ""

        where_clauses = []
        if start_date_limit is not None:
            where_clauses.append(f"datetime >= toDateTime('{start_date_limit}')")
        if end_date_limit is not None:
            where_clauses.append(f"datetime < toDateTime('{end_date_limit}')")

        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses) + " "
        else:
            where_sql = ""

        query = (
            f"SELECT "
            f"    toDateTime({kline_size} * intDiv(toUnixTimestamp(datetime), {kline_size})) AS datetime, "
            f"    argMin(price, trade_id)       AS open, "
            f"    max(price)                    AS high, "
            f"    min(price)                    AS low, "
            f"    argMax(price, trade_id)       AS close, "
            f"    avg(price)                    AS mean, "
            f"    stddevPopStable(price)        AS std, "
            f"    quantileExact(0.5)(price)     AS median, "
            f"    quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr, "
            f"    sumKahan(quantity)            AS volume, "
            f"    avg(is_buyer_maker)           AS maker_ratio, "
            f"    count()                       AS no_of_trades, "
            f"    argMin(price * quantity, trade_id)    AS open_liquidity, "
            f"    max(price * quantity)         AS high_liquidity, "
            f"    min(price * quantity)         AS low_liquidity, "
            f"    argMax(price * quantity, trade_id)    AS close_liquidity, "
            f"    sum(price * quantity)         AS liquidity_sum, "
            f"    sumKahan(is_buyer_maker * quantity)   AS maker_volume, "
            f"    sum(is_buyer_maker * price * quantity) AS maker_liquidity "
            f"FROM tdw.{table_name} "
            f"{where_sql}"
            f"GROUP BY datetime "
            f"ORDER BY datetime ASC "
            f"{limit}"
        )

        start = time.time()
        arrow_table = client.query_arrow(query)
        polars_df = pl.from_arrow(arrow_table)

        polars_df = polars_df.with_columns([
            (pl.col("datetime").cast(pl.Int64) * 1000)
            .cast(pl.Datetime("ms", time_zone="UTC"))
            .alias("datetime")
        ])

        polars_df = polars_df.with_columns([
            pl.col("mean").round(5),
            pl.col("std").round(6),
            pl.col("volume").round(9),
            pl.col("liquidity_sum").round(1),
            pl.col("maker_liquidity").round(1),
        ])

        polars_df = polars_df.sort("datetime")

        if show_summary:
            elapsed = time.time() - start
            logger.info(
                "%s s | %d rows | %d cols | %.2f GB RAM",
                f"{elapsed:.2f}",
                polars_df.shape[0],
                polars_df.shape[1],
                polars_df.estimated_size() / (1024 ** 3),
            )

        return polars_df
    finally:
        client.close()
