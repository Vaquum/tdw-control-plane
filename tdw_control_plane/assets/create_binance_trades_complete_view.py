import os
from clickhouse_driver import Client as ClickhouseClient
from dagster import asset, AssetExecutionContext

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "tdw")


def _get_clickhouse_password():
    if not CLICKHOUSE_PASSWORD:
        raise RuntimeError(
            "CLICKHOUSE_PASSWORD environment variable must be set before creating the ClickHouse client."
        )

    return CLICKHOUSE_PASSWORD


@asset(
    group_name="tdw_setup",
    description="Creates a view that combines final monthly Binance trades with open-period daily trades",
)
def create_binance_trades_complete_view(context: AssetExecutionContext):
    """
    Creates a plain ClickHouse view over monthly final data and the still-open daily overlay.
    """
    client = None
    try:
        client = ClickhouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=_get_clickhouse_password(),
            database=CLICKHOUSE_DATABASE,
        )

        db_exists = client.execute(
            f"SELECT count() FROM system.databases WHERE name = '{CLICKHOUSE_DATABASE}'"
        )
        if not db_exists[0][0]:
            context.log.error(
                f"Database {CLICKHOUSE_DATABASE} does not exist. Please create it first."
            )
            return {
                "status": "error",
                "message": f"Database {CLICKHOUSE_DATABASE} does not exist",
            }

        view_exists = client.execute(
            f"SELECT count() FROM system.tables WHERE database = '{CLICKHOUSE_DATABASE}' "
            "AND name = 'binance_trades_complete'"
        )
        was_dropped = False

        if view_exists[0][0]:
            context.log.info(
                f"View {CLICKHOUSE_DATABASE}.binance_trades_complete already exists. Dropping it..."
            )
            client.execute(f"DROP VIEW IF EXISTS {CLICKHOUSE_DATABASE}.binance_trades_complete")
            context.log.info(
                f"View {CLICKHOUSE_DATABASE}.binance_trades_complete has been dropped."
            )
            was_dropped = True

        context.log.info(f"Creating view {CLICKHOUSE_DATABASE}.binance_trades_complete...")
        client.execute(
            f"""
            CREATE VIEW {CLICKHOUSE_DATABASE}.binance_trades_complete AS
            SELECT
                trade_id,
                price,
                quantity,
                quote_quantity,
                timestamp,
                is_buyer_maker,
                is_best_match,
                datetime
            FROM {CLICKHOUSE_DATABASE}.binance_trades

            UNION ALL

            SELECT
                daily.trade_id,
                daily.price,
                daily.quantity,
                daily.quote_quantity,
                daily.timestamp,
                daily.is_buyer_maker,
                daily.is_best_match,
                daily.datetime
            FROM {CLICKHOUSE_DATABASE}.binance_daily_trades AS daily
            WHERE toStartOfMonth(daily.datetime) > ifNull(
                (
                    SELECT toStartOfMonth(max(datetime))
                    FROM {CLICKHOUSE_DATABASE}.binance_trades
                ),
                toDateTime('1969-12-01 00:00:00')
            )
        """
        )
        context.log.info(
            f"View {CLICKHOUSE_DATABASE}.binance_trades_complete has been created successfully."
        )

        return {
            "status": "success",
            "view": f"{CLICKHOUSE_DATABASE}.binance_trades_complete",
            "action": "recreated" if was_dropped else "created",
        }

    except Exception as e:
        context.log.error(f"Error creating binance_trades_complete view: {str(e)}")
        return {"status": "error", "message": str(e)}

    finally:
        if client:
            try:
                client.disconnect()
            except Exception as e:
                context.log.warning(f"Error disconnecting from ClickHouse: {str(e)}")
