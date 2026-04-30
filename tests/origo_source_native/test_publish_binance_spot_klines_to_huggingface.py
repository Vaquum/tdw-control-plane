from __future__ import annotations

import importlib
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest
from dagster import AssetKey, materialize
from polars.testing import assert_frame_equal

from .helpers import ORIGO_DATABASE


_HF_DATASET_COLUMNS = [
    'datetime',
    'open',
    'high',
    'low',
    'close',
    'mean',
    'std',
    'volume',
    'maker_ratio',
    'no_of_trades',
    'open_liquidity',
    'high_liquidity',
    'low_liquidity',
    'close_liquidity',
    'liquidity_sum',
    'maker_volume',
    'maker_liquidity',
]


def _reload_publish_module() -> ModuleType:
    sys.modules.pop(
        'tdw_control_plane.assets.publish_binance_spot_klines_to_huggingface', None
    )
    return importlib.import_module(
        'tdw_control_plane.assets.publish_binance_spot_klines_to_huggingface'
    )


def test_publish_sensor_targets_origo_spot_kline_materialization(
    origo_definitions_module,
) -> None:
    repository_def = origo_definitions_module.defs.get_repository_def()
    sensor_def = repository_def.get_sensor_def(
        'publish_binance_spot_klines_to_huggingface_sensor'
    )

    assert sensor_def.asset_key == AssetKey('refresh_binance_spot_klines_origo')
    assert sensor_def.job.name == 'publish_binance_spot_klines_to_huggingface_job'


def test_publish_snapshot_reads_origo_spot_klines(
    origo_test_env,
    origo_assets,
    materialize_binance_spot_data_source_assets,
    query_origo,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    partition_key = '2024-01-01'

    first_day_result = materialize_binance_spot_data_source_assets(partition_key=partition_key)
    assert first_day_result.success
    next_day_result = materialize_binance_spot_data_source_assets(partition_key='2024-01-02')
    assert next_day_result.success

    next_day_rows = query_origo(
        f"""
        SELECT count()
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE datetime >= toDateTime('2024-01-02 00:00:00')
          AND datetime <  toDateTime('2024-01-03 00:00:00')
        """
    )
    assert next_day_rows[0][0] > 0, (
        'Origo projection must contain rows beyond the requested partition '
        'so the partition-bound assertion below is meaningful.'
    )

    origo_projection_rows = query_origo(
        f"""
        SELECT {", ".join(_HF_DATASET_COLUMNS)}
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE datetime >= toDateTime('2020-01-01 00:00:00')
          AND datetime <  toDateTime('2024-01-02 00:00:00')
        ORDER BY datetime
        """
    )
    expected_df = pl.DataFrame(
        origo_projection_rows, schema=_HF_DATASET_COLUMNS, orient='row'
    )
    assert expected_df.height > 0

    publish_module = _reload_publish_module()

    monkeypatch.setenv('HF_TOKEN', 'test-token')
    monkeypatch.setenv('HUGGINGFACE_DATASET_REPO_ID', 'vaquum/test-binance-klines')

    upload_calls: list[dict[str, object]] = []
    create_calls: list[dict[str, object]] = []

    class FakeHfApi:
        def __init__(self, token: str | None = None, **_kwargs: object) -> None:
            self.token = token

        def create_repo(self, **kwargs: object) -> None:
            create_calls.append(kwargs)

        def upload_folder(self, **kwargs: object) -> None:
            folder_path = kwargs['folder_path']
            assert isinstance(folder_path, str)
            folder = Path(folder_path)
            captured: dict[str, object] = dict(kwargs)
            for parquet_path in folder.glob('*.parquet'):
                captured['_parquet_bytes'] = parquet_path.read_bytes()
                captured['_parquet_name'] = parquet_path.name
            captured['_readme_text'] = (folder / 'README.md').read_text(encoding='utf-8')
            upload_calls.append(captured)

    monkeypatch.setattr(publish_module, 'HfApi', FakeHfApi)

    publish_result = materialize(
        [publish_module.publish_binance_spot_klines_to_huggingface],
        partition_key=partition_key,
    )

    assert publish_result.success
    assert len(create_calls) == 1
    assert create_calls[0]['repo_id'] == 'vaquum/test-binance-klines'
    assert create_calls[0]['repo_type'] == 'dataset'

    assert len(upload_calls) == 1
    upload = upload_calls[0]
    assert upload['repo_id'] == 'vaquum/test-binance-klines'
    assert upload['repo_type'] == 'dataset'
    assert upload['_parquet_name'] == 'btcusdt_1m_kline_20200101_to_20240101.parquet'
    readme_text = upload['_readme_text']
    assert isinstance(readme_text, str)
    assert 'origo.binance_spot_klines' in readme_text
    assert 'tdw.binance_trades_complete' not in readme_text

    parquet_bytes = upload['_parquet_bytes']
    assert isinstance(parquet_bytes, bytes) and len(parquet_bytes) > 0

    snapshot_path = tmp_path / 'replay.parquet'
    snapshot_path.write_bytes(parquet_bytes)
    df = pl.read_parquet(snapshot_path)

    assert df.columns == _HF_DATASET_COLUMNS
    assert 'median' not in df.columns
    assert 'iqr' not in df.columns

    next_partition_boundary = datetime(2024, 1, 2)
    assert df.filter(pl.col('datetime') >= next_partition_boundary).height == 0, (
        'Published parquet contained rows beyond the requested daily partition; '
        'the implementation must honor end_date_limit.'
    )
    assert_frame_equal(df, expected_df, check_dtypes=False)
