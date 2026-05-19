from dagster import AssetExecutionContext, asset

from tdw_control_plane.assets.daily_trades_to_origo import daily_partitions
from tdw_control_plane.utils.publish_binance_spot_kline_snapshot_to_huggingface import (
    publish_binance_spot_kline_snapshot_to_huggingface,
)


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Exports daily BTCUSDT 30m spot klines from origo.binance_daily_spot_trades and publishes the latest snapshot to Hugging Face.",
)
def publish_binance_spot_30m_klines_to_huggingface(
    context: AssetExecutionContext,
) -> dict[str, object]:
    return publish_binance_spot_kline_snapshot_to_huggingface(
        context,
        kline_size_seconds=1800,
        file_prefix="btcusdt_30m_kline_20200101_to_",
        default_repo_id="vaquum/binance_btcusdt_30m_klines",
        repo_id_env=None,
        cadence_label="30m",
        resolution_label="30-minute",
    )
