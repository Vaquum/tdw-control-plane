from dagster import AssetExecutionContext, asset

from origo.assets.daily_trades_to_origo import daily_partitions
from origo.utils.publish_binance_spot_kline_snapshot_to_huggingface import (
    publish_binance_spot_kline_snapshot_to_huggingface,
)


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Exports daily BTCUSDT 1m spot klines from origo.binance_daily_spot_trades and publishes the latest snapshot to Hugging Face.",
)
def publish_binance_spot_klines_to_huggingface(
    context: AssetExecutionContext,
) -> dict[str, object]:
    return publish_binance_spot_kline_snapshot_to_huggingface(
        context,
        kline_size_seconds=60,
        file_prefix="btcusdt_1m_kline_20200101_to_",
        default_repo_id="vaquum/binance_btcusdt_1m_klines",
        repo_id_env="HUGGINGFACE_DATASET_REPO_ID",
        cadence_label="1m",
        resolution_label="1-minute",
    )
