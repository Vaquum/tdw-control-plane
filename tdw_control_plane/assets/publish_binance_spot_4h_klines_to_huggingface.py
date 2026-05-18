from dagster import AssetExecutionContext, asset

from tdw_control_plane.assets.daily_trades_to_origo import daily_partitions
from tdw_control_plane.utils.publish_binance_spot_kline_snapshot_to_huggingface import (
    publish_binance_spot_kline_snapshot_to_huggingface,
)


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Exports daily BTCUSDT 4h spot klines from origo.binance_daily_spot_trades and publishes the latest snapshot to Hugging Face.",
)
def publish_binance_spot_4h_klines_to_huggingface(
    context: AssetExecutionContext,
) -> dict[str, object]:
    return publish_binance_spot_kline_snapshot_to_huggingface(
        context,
        kline_size_seconds=14400,
        file_prefix="btcusdt_4h_kline_20200101_to_",
        default_repo_id="vaquum/binance_btcusdt_4h_klines",
        repo_id_env="HUGGINGFACE_4H_DATASET_REPO_ID",
        cadence_label="4h",
        resolution_label="4-hour",
    )
