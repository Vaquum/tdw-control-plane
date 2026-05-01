from __future__ import annotations

import importlib
import json
from pathlib import Path

import polars as pl
import pytest
from dagster import AssetKey, materialize

from .helpers import ORIGO_DATABASE


def test_publish_sensor_targets_origo_spot_kline_materialization(
    origo_definitions_module: object,
) -> None:
    sensor_def = origo_definitions_module.publish_binance_spot_klines_to_huggingface_sensor

    assert sensor_def.asset_key == AssetKey("refresh_binance_spot_klines_origo")


def test_publish_snapshot_reads_origo_spot_klines(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_data_source_assets: object,
    query_origo: object,
    origo_assets: dict[str, object],
) -> None:
    partition_key = "2024-01-01"
    uploaded: dict[str, object] = {}

    result = materialize_binance_spot_data_source_assets(partition_key=partition_key)
    assert result.success

    publish_module = importlib.import_module(
        "tdw_control_plane.assets.publish_binance_spot_klines_to_huggingface"
    )

    class RecordingHfApi:
        def __init__(self, token: str) -> None:
            uploaded["token"] = token

        def create_repo(self, *, repo_id: str, repo_type: str, exist_ok: bool) -> None:
            uploaded["repo_id"] = repo_id
            uploaded["repo_type"] = repo_type
            uploaded["exist_ok"] = exist_ok

        def upload_folder(
            self,
            *,
            folder_path: str,
            repo_id: str,
            repo_type: str,
            commit_message: str,
            delete_patterns: list[str],
        ) -> None:
            folder = Path(folder_path)
            metadata = json.loads((folder / "latest.json").read_text())
            uploaded["upload_repo_id"] = repo_id
            uploaded["upload_repo_type"] = repo_type
            uploaded["commit_message"] = commit_message
            uploaded["delete_patterns"] = delete_patterns
            uploaded["readme"] = (folder / "README.md").read_text()
            uploaded["metadata"] = metadata
            uploaded["parquet"] = pl.read_parquet(folder / metadata["file_name"])

    monkeypatch.setenv("HF_TOKEN", "test-token")
    monkeypatch.setenv("HUGGINGFACE_DATASET_REPO_ID", "test/binance-klines")
    monkeypatch.setattr(publish_module, "HfApi", RecordingHfApi)

    publish_result = materialize(
        [publish_module.publish_binance_spot_klines_to_huggingface],
        partition_key=partition_key,
    )
    assert publish_result.success

    parquet = uploaded["parquet"]
    metadata = uploaded["metadata"]
    readme = uploaded["readme"]
    row_count = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE datetime >= toDateTime('2020-01-01 00:00:00')
          AND datetime < toDateTime('2024-01-02 00:00:00')
        """
    )[0][0]

    assert isinstance(parquet, pl.DataFrame)
    assert isinstance(metadata, dict)
    assert isinstance(readme, str)
    assert uploaded["token"] == "test-token"
    assert uploaded["repo_id"] == "test/binance-klines"
    assert uploaded["upload_repo_id"] == "test/binance-klines"
    assert metadata["export_end_date"] == partition_key
    assert metadata["row_count"] == row_count
    assert parquet.height == row_count
    assert parquet.columns == [
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "mean",
        "std",
        "volume",
        "maker_ratio",
        "no_of_trades",
        "open_liquidity",
        "high_liquidity",
        "low_liquidity",
        "close_liquidity",
        "liquidity_sum",
        "maker_volume",
        "maker_liquidity",
    ]
    assert "origo.binance_spot_klines" in readme
