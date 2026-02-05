# Changelog

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

## v1.0.11 on 6th of January, 2026

- Migrated from `setup.py` to `pyproject.toml`

## v1.0.12 on 9th of January, 2026

- Implemented secure secrets management via .env file injection
- Removed hardcoded passwords from docker-compose configuration
- Added GitHub Workflow for secure environment variable deployment

## v1.0.13 on 19th of January, 2026

- Fixed dagster deployment errors by restoring `check_if_has_header.py` utility.
- Migrated dependency from `Loop` to `vaquum_limen` in `monthly_futures_agg_trades_to_tdw`.

## v1.0.14 on 3rd of February, 2026

- Added Keyboard Shortcuts for JupyterLab to `run-all-cells` and `restart-kernal-and-run-all-cells`

## v1.1.0 on 3rd of February, 2026

- Removed `tabix` and `metabase` images from docker
- Removed `metbase-data` and `grafana-data` volumes from docker

## v1.1.1 on 4th of February, 2026

- Enhanced daily_trades_to_tdw.py to achieve feature parity with monthly_trades_to_tdw.py
- Added robust error handling and resource cleanup with explicit client disconnection
- Implemented duplicate data handling with automatic deletion before insertion
- Added statistical verification approach with lightweight checks
- Enhanced timestamp handling to support both millisecond and microsecond formats
- Updated configuration to match monthly script patterns
- Created new dagit job for creating db `origo` and a table for it `binance_trades`
