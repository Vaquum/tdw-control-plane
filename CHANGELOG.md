# Changelog

## 16:23 on 09-08-2024

- Versioned release with `clickhouse-cityhash` moved to core dependencies
- Bumped version from 1.0.2 to 1.0.3

## 11:38 on 31-07-2024

- Updated comment in daily_trades_to_tdw.py to correctly reflect that compression is enabled with clickhouse-cityhash

## 12:45 on 25-07-2024

- Added 5-minute timeout to ClickHouse client connection to improve error handling 

## 16:27 on 28-05-2024

- Fixed Clickhouse compression dependency by moving `clickhouse-cityhash` from optional extras to required dependencies
- Bumped version from 1.0.1 to 1.0.2 