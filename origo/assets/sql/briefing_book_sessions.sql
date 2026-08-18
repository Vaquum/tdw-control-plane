-- 8-hour UTC session aggregates (00-08, 08-16, 16-24) of the depth20 1m book
-- for the UTC day {day:Date}. minutes counts the 1m rows behind each session
-- so a thinly covered session is visible instead of passing as a quiet one.
SELECT
    toDateTime(28800 * intDiv(toUnixTimestamp(datetime), 28800)) AS session_start,
    count() AS minutes,
    argMin(book_mid_price, datetime) AS open_mid_price,
    argMax(book_mid_price, datetime) AS close_mid_price,
    avg(book_spread_bps) AS avg_spread_bps,
    avg(book_bid_depth_20_notional) AS avg_bid_depth_20_notional,
    avg(book_ask_depth_20_notional) AS avg_ask_depth_20_notional,
    avg(book_imbalance_20) AS avg_imbalance_20
FROM origo.binance_spot_depth20_1m FINAL
WHERE datetime >= toDateTime({day:Date})
  AND datetime < toDateTime({day:Date} + 1)
GROUP BY session_start
ORDER BY session_start
