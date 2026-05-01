from datetime import datetime
import logging
import re
import time

import polars as pl

from tdw_control_plane.utils.get_clickhouse_client import get_clickhouse_client

logger = logging.getLogger(__name__)
_SUPPORTED_DATETIME_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)


def _validate_positive_int(value: int | None, field_name: str) -> int | None:
    if value is None:
        return None

    if type(value) is not int:
        raise TypeError(f"{field_name} must be an int.")

    if value < 1:
        raise ValueError(f"{field_name} must be at least 1.")

    return value


def _normalize_datetime_literal(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None

    for fmt in _SUPPORTED_DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

    raise ValueError(
        f"{field_name} must match one of: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS."
    )


def get_binance_spot_klines(
    n_rows: int | None = None,
    kline_size: int = 1,
    start_date_limit: str | None = None,
    end_date_limit: str | None = None,
    show_summary: bool = False,
    table_name: str = "binance_trades_complete",
    database_name: str = "tdw",
    include_quantiles: bool = True,
) -> pl.DataFrame:
    """Query Binance BTCUSDT spot klines from TDW.

    This is TDW's canonical spot-kline query and mirrors the existing
    HistoricalData.get_spot_klines semantics while sourcing data from
    tdw.binance_trades_complete by default.

    Args:
        n_rows: Optional maximum number of rows to return.
        kline_size: Bucket width in seconds used for kline aggregation.
            For example, ``60`` means 1-minute klines.
        start_date_limit: Optional inclusive lower datetime bound. Accepts
            `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DDTHH:MM:SS`.
        end_date_limit: Optional exclusive upper datetime bound. Accepts
            `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS`, or `YYYY-MM-DDTHH:MM:SS`.
        show_summary: Whether to log query timing and dataframe size details.
        table_name: ClickHouse source table name.
        database_name: ClickHouse source database name.
        include_quantiles: Whether to include `median` and `iqr` columns.
    """

    if re.fullmatch(r"[A-Za-z0-9_]+", table_name) is None:
        raise ValueError(f"Invalid ClickHouse table name: {table_name}")
    if re.fullmatch(r"[A-Za-z0-9_]+", database_name) is None:
        raise ValueError(f"Invalid ClickHouse database name: {database_name}")

    n_rows = _validate_positive_int(n_rows, "n_rows")
    kline_size = _validate_positive_int(kline_size, "kline_size")
    start_date_limit = _normalize_datetime_literal(start_date_limit, "start_date_limit")
    end_date_limit = _normalize_datetime_literal(end_date_limit, "end_date_limit")

    client = get_clickhouse_client()
    try:
        query_parameters: dict[str, int | str] = {
            "bucket_seconds": kline_size,
        }
        limit = "LIMIT {limit_rows:UInt64}" if n_rows is not None else ""
        if n_rows is not None:
            query_parameters["limit_rows"] = n_rows

        where_clauses = []
        if start_date_limit is not None:
            where_clauses.append("datetime >= toDateTime({start_dt:String})")
            query_parameters["start_dt"] = start_date_limit
        if end_date_limit is not None:
            where_clauses.append("datetime < toDateTime({end_dt:String})")
            query_parameters["end_dt"] = end_date_limit

        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses) + " "
        else:
            where_sql = ""

        quantile_sql = ""
        if include_quantiles:
            quantile_sql = (
                "    quantileExact(0.5)(price)     AS median, "
                "    quantileExact(0.75)(price) - quantileExact(0.25)(price) AS iqr, "
            )

        query = (
            f"SELECT "
            f"    toDateTime({{bucket_seconds:UInt32}} * intDiv(toUnixTimestamp(datetime), {{bucket_seconds:UInt32}})) AS datetime, "
            f"    argMin(price, trade_id)       AS open, "
            f"    max(price)                    AS high, "
            f"    min(price)                    AS low, "
            f"    argMax(price, trade_id)       AS close, "
            f"    avg(price)                    AS mean, "
            f"    stddevPopStable(price)        AS std, "
            f"{quantile_sql}"
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
            f"FROM {database_name}.{table_name} "
            f"{where_sql}"
            f"GROUP BY datetime "
            f"ORDER BY datetime ASC "
            f"{limit}"
        )

        start = time.time()
        arrow_table = client.query_arrow(query, parameters=query_parameters)
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
