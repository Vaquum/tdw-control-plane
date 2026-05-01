from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from pathlib import Path

import polars as pl
import pytest
from dagster import AssetKey, materialize

from .helpers import ORIGO_DATABASE

KLINE_EXPORT_COLUMNS = [
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


def _origo_projection_dataframe(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    table_name: object,
    partition_key: str,
) -> pl.DataFrame:
    rows = query_origo(
        f"""
        SELECT
            {', '.join(KLINE_EXPORT_COLUMNS)}
        FROM {ORIGO_DATABASE}.{table_name}
        WHERE datetime >= toDateTime('2020-01-01 00:00:00')
          AND datetime < toDateTime('2024-01-02 00:00:00')
        ORDER BY datetime
        """
    )
    return pl.DataFrame(rows, schema=KLINE_EXPORT_COLUMNS, orient="row")


def test_publish_sensor_targets_origo_spot_kline_materialization(
    origo_definitions_module: object,
) -> None:
    sensor_def = origo_definitions_module.publish_binance_spot_klines_to_huggingface_sensor

    assert sensor_def.asset_key == AssetKey("refresh_binance_spot_klines_origo")


def test_publish_snapshot_reads_origo_spot_trades_with_shared_query(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_data_source_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
    origo_assets: dict[str, object],
) -> None:
    partition_key = "2024-01-01"
    uploaded: dict[str, object] = {}
    captured_query: dict[str, object] = {}

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

    def recording_get_binance_spot_klines(**kwargs: object) -> pl.DataFrame:
        captured_query.update(kwargs)
        return _origo_projection_dataframe(
            query_origo,
            origo_assets["KLINES_TABLE_NAME"],
            partition_key,
        )

    monkeypatch.setenv("HF_TOKEN", "test-token")
    monkeypatch.setenv("HUGGINGFACE_DATASET_REPO_ID", "test/binance-klines")
    monkeypatch.setattr(publish_module, "HfApi", RecordingHfApi)
    monkeypatch.setattr(
        publish_module,
        "get_binance_spot_klines",
        recording_get_binance_spot_klines,
    )

    publish_result = materialize(
        [publish_module.publish_binance_spot_klines_to_huggingface],
        partition_key=partition_key,
    )
    assert publish_result.success

    parquet = uploaded["parquet"]
    metadata = uploaded["metadata"]
    readme = uploaded["readme"]

    assert isinstance(parquet, pl.DataFrame)
    assert isinstance(metadata, dict)
    assert isinstance(readme, str)
    assert uploaded["token"] == "test-token"
    assert uploaded["repo_id"] == "test/binance-klines"
    assert uploaded["upload_repo_id"] == "test/binance-klines"
    assert metadata["export_end_date"] == partition_key
    assert metadata["row_count"] == parquet.height
    assert parquet.columns == KLINE_EXPORT_COLUMNS
    assert captured_query == {
        "kline_size": 60,
        "start_date_limit": "2020-01-01 00:00:00",
        "end_date_limit": "2024-01-02 00:00:00",
        "table_name": "binance_daily_spot_trades",
        "database_name": "origo",
        "include_quantiles": False,
    }
    assert "origo.binance_daily_spot_trades" in readme
