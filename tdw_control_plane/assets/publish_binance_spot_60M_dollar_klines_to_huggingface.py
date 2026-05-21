from dagster import AssetExecutionContext, asset

from tdw_control_plane.assets.daily_trades_to_origo import daily_partitions
from tdw_control_plane.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface import (
    publish_binance_spot_dollar_kline_snapshot_to_huggingface,
)


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Exports daily BTCUSDT 60M dollar spot klines from origo.binance_spot_dollar_klines and publishes the latest snapshot to Hugging Face.",
)
def publish_binance_spot_60M_dollar_klines_to_huggingface(
    context: AssetExecutionContext,
) -> dict[str, object]:
    return publish_binance_spot_dollar_kline_snapshot_to_huggingface(
        context,
        dollar_size=60_000_000.0,
        file_prefix="btcusdt_60M_dollar_kline_20200101_to_",
        default_repo_id="vaquum/binance_btcusdt_60M_dollar_klines",
        repo_id_env=None,
        size_label="60M",
        resolution_label="60M-dollar",
    )
