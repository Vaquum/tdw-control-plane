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

## 12:22 on 19-08-2025

- Added Keyboard Shortcuts for JupyterLab to `run-all-cells` and `restart-kernal-and-run-all-cells`

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
