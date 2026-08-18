-- Per-minute book state for the UTC day {day:Date} from the depth20 1m
-- projection. FINAL collapses ReplacingMergeTree duplicates to the latest
-- snapshot per minute.
SELECT
    datetime,
    book_mid_price,
    book_spread_bps,
    book_bid_depth_20_notional,
    book_ask_depth_20_notional,
    book_imbalance_20
FROM origo.binance_spot_depth20_1m FINAL
WHERE datetime >= toDateTime({day:Date})
  AND datetime < toDateTime({day:Date} + 1)
ORDER BY datetime
