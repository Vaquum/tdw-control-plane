-- One OHLCV bar per {bucket_seconds:UInt32} bucket of the UTC day {day:Date},
-- rolled up from the 1m origo.binance_spot_klines projection. source_minutes
-- counts the 1m rows behind each bar so the caller can refuse a short bar
-- instead of publishing it.
SELECT
    toDateTime({bucket_seconds:UInt32} * intDiv(toUnixTimestamp(datetime), {bucket_seconds:UInt32})) AS bar_start,
    argMin(open, datetime) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, datetime) AS close,
    sumKahan(volume) AS volume,
    sum(no_of_trades) AS no_of_trades,
    count() AS source_minutes
FROM origo.binance_spot_klines
WHERE datetime >= toDateTime({day:Date})
  AND datetime < toDateTime({day:Date} + 1)
GROUP BY bar_start
ORDER BY bar_start
