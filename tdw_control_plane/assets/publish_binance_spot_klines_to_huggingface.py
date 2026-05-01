import hashlib
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dagster import AssetExecutionContext, asset
from huggingface_hub import HfApi

from tdw_control_plane.assets.daily_trades_to_origo import daily_partitions
from tdw_control_plane.query import get_binance_spot_klines

EXPORT_START_DATE = "2020-01-01 00:00:00"
EXPORT_FILENAME_PREFIX = "btcusdt_1m_kline_20200101_to_"


def _get_huggingface_token() -> str:
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
    if not token:
        raise RuntimeError(
            "HF_TOKEN or HUGGINGFACE_HUB_TOKEN must be set before publishing to Hugging Face."
        )

    return token


def _get_huggingface_dataset_repo_id() -> str:
    return os.environ.get(
        "HUGGINGFACE_DATASET_REPO_ID",
        "vaquum/binance_btcusdt_1m_klines",
    )


def _build_dataset_card(export_end_date: str, row_count: int, file_name: str) -> str:
    return f"""# BTCUSDT 1m spot klines

This dataset is exported daily from `origo.binance_daily_spot_trades` using `get_binance_spot_klines` at 1-minute resolution.

Latest snapshot:

- file: `{file_name}`
- start date: `2020-01-01`
- rows: `{row_count}`
- end date: `{export_end_date}`
- columns: `datetime`, `open`, `high`, `low`, `close`, `mean`, `std`, `volume`, `maker_ratio`, `no_of_trades`, `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`, `liquidity_sum`, `maker_volume`, `maker_liquidity`

Notes:

- Source market: Binance spot BTCUSDT
- Source table: `origo.binance_daily_spot_trades`
- `median` and `iqr` are intentionally omitted from the exported Parquet snapshot
- Timestamps are UTC
"""


def _build_snapshot_metadata(
    export_end_date: str,
    file_name: str,
    row_count: int,
    file_sha256: str,
) -> str:
    generated_at = datetime.now(timezone.utc).isoformat()
    return (
        json.dumps(
            {
                "file_name": file_name,
                "export_start_date": "2020-01-01",
                "export_end_date": export_end_date,
                "row_count": row_count,
                "sha256": file_sha256,
                "generated_at_utc": generated_at,
            },
            indent=2,
        )
        + "\n"
    )


def _sha256_for_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


@asset(
    partitions_def=daily_partitions,
    group_name="binance_data",
    description="Exports daily BTCUSDT 1m spot klines from origo.binance_daily_spot_trades and publishes the latest snapshot to Hugging Face.",
)
def publish_binance_spot_klines_to_huggingface(
    context: AssetExecutionContext,
) -> dict[str, object]:
    partition_date_str = context.asset_partition_key_for_output()
    if partition_date_str is None:
        raise RuntimeError(
            "publish_binance_spot_klines_to_huggingface requires a daily partition key."
        )

    export_end_date = partition_date_str
    export_end_exclusive = (
        datetime.strptime(export_end_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d 00:00:00")
    dataset_repo_id = _get_huggingface_dataset_repo_id()

    file_name = (
        f"{EXPORT_FILENAME_PREFIX}{export_end_date.replace('-', '')}.parquet"
    )

    context.log.info(
        f"Building Binance spot klines snapshot through {export_end_date} UTC."
    )
    data = get_binance_spot_klines(
        kline_size=60,
        start_date_limit=EXPORT_START_DATE,
        end_date_limit=export_end_exclusive,
        table_name="binance_daily_spot_trades",
        database_name="origo",
        include_quantiles=False,
    )

    if data.height == 0:
        raise RuntimeError(
            f"No kline rows were returned for export through {export_end_date}."
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        parquet_path = tmp_path / file_name
        readme_path = tmp_path / "README.md"
        metadata_path = tmp_path / "latest.json"

        context.log.info(f"Writing snapshot to {parquet_path}.")
        data.write_parquet(parquet_path, compression="zstd")
        file_sha256 = _sha256_for_file(parquet_path)

        readme_path.write_text(
            _build_dataset_card(
                export_end_date=export_end_date,
                row_count=data.height,
                file_name=file_name,
            ),
            encoding="utf-8",
        )
        metadata_path.write_text(
            _build_snapshot_metadata(
                export_end_date=export_end_date,
                file_name=file_name,
                row_count=data.height,
                file_sha256=file_sha256,
            ),
            encoding="utf-8",
        )

        api = HfApi(token=_get_huggingface_token())
        api.create_repo(
            repo_id=dataset_repo_id,
            repo_type="dataset",
            exist_ok=True,
        )

        commit_message = f"Add BTCUSDT 1m klines snapshot through {export_end_date}"
        api.upload_folder(
            folder_path=str(tmp_path),
            repo_id=dataset_repo_id,
            repo_type="dataset",
            commit_message=commit_message,
            delete_patterns=[f"{EXPORT_FILENAME_PREFIX}*.parquet"],
        )

    latest_datetime = data["datetime"].max()
    context.log.info(
        f"Published {file_name} to {dataset_repo_id} with {data.height} rows."
    )

    return {
        "repo_id": dataset_repo_id,
        "file_name": file_name,
        "rows_exported": data.height,
        "latest_datetime": str(latest_datetime),
        "sha256": file_sha256,
    }
