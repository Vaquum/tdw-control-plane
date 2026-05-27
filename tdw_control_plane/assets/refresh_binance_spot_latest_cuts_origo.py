from datetime import UTC, datetime, timedelta

from dagster import AssetExecutionContext, asset

from .create_binance_spot_latest_tables_origo import (
    LATEST_DOLLAR_CUT_TABLES,
    LATEST_DOLLAR_KLINES_TABLE_NAME,
    LATEST_KLINES_TABLE_NAME,
    LATEST_TIME_CUT_TABLES,
    create_binance_spot_latest_tables_origo,
)
from .create_origo_database import (
    ClickHouseClientProtocol,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from .refresh_binance_spot_dollar_klines_latest_origo import (
    refresh_binance_spot_dollar_klines_latest_origo,
)
from .refresh_binance_spot_klines_latest_origo import refresh_binance_spot_klines_latest_origo
from .sync_binance_spot_trades_latest_origo import latest_minute_from_context


def _window_start(minute_start: datetime, cadence_minutes: int) -> datetime:
    aware = minute_start if minute_start.tzinfo is not None else minute_start.replace(tzinfo=UTC)
    window_seconds = cadence_minutes * 60
    timestamp = int(aware.timestamp())
    return datetime.fromtimestamp(timestamp - (timestamp % window_seconds), tz=UTC).replace(
        tzinfo=None
    )


def _delete_time_cut_window(
    client: ClickHouseClientProtocol,
    database: str,
    table_name: str,
    window_start: datetime,
) -> None:
    client.execute(
        f"""
        ALTER TABLE {database}.{table_name}
        DELETE WHERE datetime = toDateTime('{window_start:%Y-%m-%d %H:%M:%S}')
        """,
        settings={'mutations_sync': 2},
    )


def _insert_time_cut_window(
    client: ClickHouseClientProtocol,
    database: str,
    table_name: str,
    cadence_minutes: int,
    window_start: datetime,
) -> None:
    window_end = window_start + timedelta(minutes=cadence_minutes)
    client.execute(
        f"""
        INSERT INTO {database}.{table_name}
        SELECT
            cut_datetime AS datetime,
            argMin(open, datetime) AS open,
            max(high) AS high,
            min(low) AS low,
            argMax(close, datetime) AS close,
            sum(mean * source_no_of_trades) / sum(source_no_of_trades) AS mean,
            sqrt(sum(std * std * source_no_of_trades) / sum(source_no_of_trades)) AS std,
            quantileExact(0.5)(median) AS median,
            quantileExact(0.75)(median) - quantileExact(0.25)(median) AS iqr,
            sumKahan(volume) AS volume,
            sum(maker_ratio * source_no_of_trades) / sum(source_no_of_trades) AS maker_ratio,
            sum(source_no_of_trades) AS no_of_trades,
            argMin(open_liquidity, datetime) AS open_liquidity,
            max(high_liquidity) AS high_liquidity,
            min(low_liquidity) AS low_liquidity,
            argMax(close_liquidity, datetime) AS close_liquidity,
            sum(liquidity_sum) AS liquidity_sum,
            sumKahan(maker_volume) AS maker_volume,
            sum(maker_liquidity) AS maker_liquidity
        FROM (
            SELECT
                datetime,
                open,
                high,
                low,
                close,
                mean,
                std,
                median,
                volume,
                maker_ratio,
                no_of_trades AS source_no_of_trades,
                open_liquidity,
                high_liquidity,
                low_liquidity,
                close_liquidity,
                liquidity_sum,
                maker_volume,
                maker_liquidity,
                toStartOfInterval(datetime, INTERVAL {cadence_minutes} MINUTE) AS cut_datetime
            FROM {database}.{LATEST_KLINES_TABLE_NAME}
            WHERE datetime >= toDateTime('{window_start:%Y-%m-%d %H:%M:%S}')
              AND datetime < toDateTime('{window_end:%Y-%m-%d %H:%M:%S}')
        )
        GROUP BY cut_datetime
        ORDER BY cut_datetime
        """
    )


def _delete_dollar_cut_window(
    client: ClickHouseClientProtocol,
    database: str,
    table_name: str,
    minute_start: datetime,
) -> None:
    minute_end = minute_start + timedelta(minutes=1)
    client.execute(
        f"""
        ALTER TABLE {database}.{table_name}
        DELETE WHERE start_datetime >= toDateTime('{minute_start:%Y-%m-%d %H:%M:%S}')
          AND start_datetime < toDateTime('{minute_end:%Y-%m-%d %H:%M:%S}')
        """,
        settings={'mutations_sync': 2},
    )


def _insert_dollar_cut_window(
    client: ClickHouseClientProtocol,
    database: str,
    table_name: str,
    ratio: int,
    minute_start: datetime,
) -> None:
    minute_end = minute_start + timedelta(minutes=1)
    client.execute(
        f"""
        INSERT INTO {database}.{table_name}
        SELECT
            min(start_datetime) AS start_datetime,
            max(end_datetime) AS end_datetime,
            cut_dollar_bar_id AS dollar_bar_id,
            argMin(open, start_datetime) AS open,
            max(high) AS high,
            min(low) AS low,
            argMax(close, end_datetime) AS close,
            sum(mean * source_no_of_trades) / sum(source_no_of_trades) AS mean,
            sqrt(sum(std * std * source_no_of_trades) / sum(source_no_of_trades)) AS std,
            quantileExact(0.5)(median) AS median,
            quantileExact(0.75)(median) - quantileExact(0.25)(median) AS iqr,
            sumKahan(volume) AS volume,
            sum(maker_ratio * source_no_of_trades) / sum(source_no_of_trades) AS maker_ratio,
            sum(source_no_of_trades) AS no_of_trades,
            argMin(open_liquidity, start_datetime) AS open_liquidity,
            max(high_liquidity) AS high_liquidity,
            min(low_liquidity) AS low_liquidity,
            argMax(close_liquidity, end_datetime) AS close_liquidity,
            sum(liquidity_sum) AS liquidity_sum,
            sumKahan(maker_volume) AS maker_volume,
            sum(maker_liquidity) AS maker_liquidity
        FROM (
            SELECT
                start_datetime,
                end_datetime,
                dollar_bar_id,
                open,
                high,
                low,
                close,
                mean,
                std,
                median,
                volume,
                maker_ratio,
                no_of_trades AS source_no_of_trades,
                open_liquidity,
                high_liquidity,
                low_liquidity,
                close_liquidity,
                liquidity_sum,
                maker_volume,
                maker_liquidity,
                toUInt64(intDiv(dollar_bar_id, {ratio})) AS cut_dollar_bar_id
            FROM {database}.{LATEST_DOLLAR_KLINES_TABLE_NAME}
            WHERE start_datetime >= toDateTime('{minute_start:%Y-%m-%d %H:%M:%S}')
              AND start_datetime < toDateTime('{minute_end:%Y-%m-%d %H:%M:%S}')
        )
        GROUP BY cut_dollar_bar_id
        ORDER BY cut_dollar_bar_id
        """
    )


@asset(
    deps=[
        create_binance_spot_latest_tables_origo,
        refresh_binance_spot_klines_latest_origo,
        refresh_binance_spot_dollar_klines_latest_origo,
    ],
    group_name='binance_data',
    description='Refreshes rolling latest time and dollar cadence cuts from foundation tables.',
)
def refresh_binance_spot_latest_cuts_origo(
    context: AssetExecutionContext,
) -> dict[str, object]:
    minute_start = latest_minute_from_context(context).replace(tzinfo=None)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        refreshed_tables: list[str] = []
        for _, table_name, cadence_minutes in LATEST_TIME_CUT_TABLES:
            start = _window_start(minute_start, cadence_minutes)
            _delete_time_cut_window(client, settings.database, table_name, start)
            _insert_time_cut_window(client, settings.database, table_name, cadence_minutes, start)
            refreshed_tables.append(table_name)

        for _, table_name, ratio in LATEST_DOLLAR_CUT_TABLES:
            _delete_dollar_cut_window(client, settings.database, table_name, minute_start)
            _insert_dollar_cut_window(client, settings.database, table_name, ratio, minute_start)
            refreshed_tables.append(table_name)

        return {
            'status': 'success',
            'minute_start': minute_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'tables': refreshed_tables,
        }
    finally:
        client.disconnect()
