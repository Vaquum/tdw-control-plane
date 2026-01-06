## 15:32 on 22-07-2024
- Added new binance_trades_hour_of_day_summary asset to create a table aggregating trade statistics by hour of day 

## 16:10 on 22-07-2024
- Added new binance_trades_day_of_month_summary asset to create a table aggregating trade statistics by day of month 

## 16:25 on 22-07-2024
- Added new binance_trades_week_of_year_summary asset to create a table aggregating trade statistics by week of year 

## 16:40 on 22-07-2024
- Added new binance_trades_month_of_year_summary asset to create a table aggregating trade statistics by month of year

## 16:55 on 22-07-2024
- Updated definitions.py to register all new summary assets and create their respective jobs

## 12:40 on 23-08-2025
- Migrated from `vaquum_tools` to `loop`

## 10:43 on 14-10-2025
- Enhanced daily_trades_to_tdw.py to achieve feature parity with monthly_trades_to_tdw.py
- Added robust error handling and resource cleanup with explicit client disconnection
- Implemented duplicate data handling with automatic deletion before insertion
- Added statistical verification approach with lightweight checks
- Enhanced timestamp handling to support both millisecond and microsecond formats
- Updated configuration to match monthly script patterns