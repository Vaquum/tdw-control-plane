def binance_spot_dollar_kline_projection_sql(
    *,
    database: str,
    source_table: str,
    datetime_predicate_sql: str,
    dollar_kline_size: float,
) -> str:
    return f"""
        SELECT
            min(datetime) AS start_datetime,
            max(datetime) AS end_datetime,
            dollar_bar_id,
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
            sum(is_buyer_maker * price * quantity) AS maker_liquidity
        FROM (
            SELECT
                *,
                toUInt64(floor(running_quote_before / {dollar_kline_size})) AS dollar_bar_id
            FROM (
                SELECT
                    *,
                    greatest(
                        sum(quote_quantity) OVER (
                            ORDER BY datetime, trade_id
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) - quote_quantity,
                        0.0
                    ) AS running_quote_before
                FROM {database}.{source_table}
                WHERE {datetime_predicate_sql}
            )
        )
        GROUP BY dollar_bar_id
        ORDER BY dollar_bar_id
        """
