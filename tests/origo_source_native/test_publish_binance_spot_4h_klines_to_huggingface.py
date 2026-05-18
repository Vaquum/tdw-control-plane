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


def _origo_trades_4h_kline_dataframe(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    *,
    database_name: str,
    table_name: str,
    start_date_limit: str,
    end_date_limit: str,
) -> pl.DataFrame:
    rows = query_origo(
        f"""
        SELECT
            kline_datetime AS datetime,
            argMin(price, trade_id) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, trade_id) AS close,
            round(avg(price), 5) AS mean,
            round(stddevPopStable(price), 6) AS std,
            round(sumKahan(quantity), 9) AS volume,
            avg(is_buyer_maker) AS maker_ratio,
            count() AS no_of_trades,
            argMin(price * quantity, trade_id) AS open_liquidity,
            max(price * quantity) AS high_liquidity,
            min(price * quantity) AS low_liquidity,
            argMax(price * quantity, trade_id) AS close_liquidity,
            round(sum(price * quantity), 1) AS liquidity_sum,
            sumKahan(is_buyer_maker * quantity) AS maker_volume,
            round(sum(is_buyer_maker * price * quantity), 1) AS maker_liquidity
        FROM (
            SELECT
                *,
                toDateTime(14400 * intDiv(toUnixTimestamp(datetime), 14400)) AS kline_datetime
            FROM {database_name}.{table_name}
            WHERE datetime >= toDateTime('{start_date_limit}')
              AND datetime < toDateTime('{end_date_limit}')
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )
    return pl.DataFrame(rows, schema=KLINE_EXPORT_COLUMNS, orient='row')


def test_publish_4h_sensor_targets_origo_spot_trades_materialization(
    origo_definitions_module: object,
) -> None:
    sensor_def = (
        origo_definitions_module.publish_binance_spot_4h_klines_to_huggingface_sensor
    )

    assert sensor_def.asset_key == AssetKey('insert_daily_binance_spot_trades_to_origo')


def test_publish_4h_snapshot_reads_origo_spot_trades_with_shared_query(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_data_source_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
) -> None:
    partition_key = '2024-01-01'
    uploaded: dict[str, object] = {}
    captured_query: dict[str, object] = {}

    result = materialize_binance_spot_data_source_assets(partition_key=partition_key)
    assert result.success

    publish_module = importlib.import_module(
        'tdw_control_plane.assets.publish_binance_spot_4h_klines_to_huggingface'
    )
    publish_helper_module = importlib.import_module(
        'tdw_control_plane.utils.publish_binance_spot_kline_snapshot_to_huggingface'
    )

    class RecordingHfApi:
        def __init__(self, token: str) -> None:
            uploaded['token'] = token

        def create_repo(self, *, repo_id: str, repo_type: str, exist_ok: bool) -> None:
            uploaded['repo_id'] = repo_id
            uploaded['repo_type'] = repo_type
            uploaded['exist_ok'] = exist_ok

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
            metadata = json.loads((folder / 'latest.json').read_text())
            uploaded['upload_repo_id'] = repo_id
            uploaded['upload_repo_type'] = repo_type
            uploaded['commit_message'] = commit_message
            uploaded['delete_patterns'] = delete_patterns
            uploaded['readme'] = (folder / 'README.md').read_text()
            uploaded['metadata'] = metadata
            uploaded['parquet'] = pl.read_parquet(folder / metadata['file_name'])

    def recording_get_binance_spot_klines(**kwargs: object) -> pl.DataFrame:
        captured_query.update(kwargs)
        database_name = kwargs.get('database_name')
        table_name = kwargs.get('table_name')
        start_date_limit = kwargs.get('start_date_limit')
        end_date_limit = kwargs.get('end_date_limit')

        assert database_name == ORIGO_DATABASE
        assert table_name == 'binance_daily_spot_trades'
        assert isinstance(start_date_limit, str)
        assert isinstance(end_date_limit, str)

        return _origo_trades_4h_kline_dataframe(
            query_origo,
            database_name=database_name,
            table_name=table_name,
            start_date_limit=start_date_limit,
            end_date_limit=end_date_limit,
        )

    monkeypatch.setenv('HF_TOKEN', 'test-token')
    monkeypatch.setenv('HUGGINGFACE_4H_DATASET_REPO_ID', 'test/binance-4h-klines')
    monkeypatch.setattr(publish_helper_module, 'HfApi', RecordingHfApi)
    monkeypatch.setattr(
        publish_helper_module,
        'get_binance_spot_klines',
        recording_get_binance_spot_klines,
    )

    publish_result = materialize(
        [publish_module.publish_binance_spot_4h_klines_to_huggingface],
        partition_key=partition_key,
    )
    assert publish_result.success

    parquet = uploaded['parquet']
    metadata = uploaded['metadata']
    readme = uploaded['readme']

    assert isinstance(parquet, pl.DataFrame)
    assert isinstance(metadata, dict)
    assert isinstance(readme, str)
    assert uploaded['token'] == 'test-token'
    assert uploaded['repo_id'] == 'test/binance-4h-klines'
    assert uploaded['upload_repo_id'] == 'test/binance-4h-klines'
    assert metadata['export_end_date'] == partition_key
    assert metadata['row_count'] == parquet.height
    assert parquet.columns == KLINE_EXPORT_COLUMNS
    assert captured_query == {
        'kline_size': 14400,
        'start_date_limit': '2020-01-01 00:00:00',
        'end_date_limit': '2024-01-02 00:00:00',
        'table_name': 'binance_daily_spot_trades',
        'database_name': 'origo',
        'include_quantiles': False,
    }
    assert uploaded['commit_message'] == 'Add BTCUSDT 4h klines snapshot through 2024-01-01'
    assert uploaded['delete_patterns'] == ['btcusdt_4h_kline_20200101_to_*.parquet']
    assert metadata['file_name'] == 'btcusdt_4h_kline_20200101_to_20240101.parquet'
    assert '4-hour resolution' in readme
    assert 'origo.binance_daily_spot_trades' in readme
