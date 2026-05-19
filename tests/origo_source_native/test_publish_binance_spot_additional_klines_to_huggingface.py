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

KLINE_CASES = [
    (
        '15m',
        '15-minute',
        900,
        'tdw_control_plane.assets.publish_binance_spot_15m_klines_to_huggingface',
        'publish_binance_spot_15m_klines_to_huggingface',
        'publish_binance_spot_15m_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_15m_klines',
        'btcusdt_15m_kline_20200101_to_',
    ),
    (
        '30m',
        '30-minute',
        1800,
        'tdw_control_plane.assets.publish_binance_spot_30m_klines_to_huggingface',
        'publish_binance_spot_30m_klines_to_huggingface',
        'publish_binance_spot_30m_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_30m_klines',
        'btcusdt_30m_kline_20200101_to_',
    ),
    (
        '2h',
        '2-hour',
        7200,
        'tdw_control_plane.assets.publish_binance_spot_2h_klines_to_huggingface',
        'publish_binance_spot_2h_klines_to_huggingface',
        'publish_binance_spot_2h_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_2h_klines',
        'btcusdt_2h_kline_20200101_to_',
    ),
]


def _origo_trades_kline_dataframe(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    *,
    kline_size: int,
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
                toDateTime({kline_size} * intDiv(toUnixTimestamp(datetime), {kline_size})) AS kline_datetime
            FROM {database_name}.{table_name}
            WHERE datetime >= toDateTime('{start_date_limit}')
              AND datetime < toDateTime('{end_date_limit}')
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )
    return pl.DataFrame(rows, schema=KLINE_EXPORT_COLUMNS, orient='row')


def test_publish_15m_30m_2h_sensors_target_origo_spot_trades_materialization(
    origo_definitions_module: object,
) -> None:
    for case in KLINE_CASES:
        sensor_name = case[5]
        sensor_def = getattr(origo_definitions_module, sensor_name)

        assert sensor_def.asset_key == AssetKey('insert_daily_binance_spot_trades_to_origo')


def test_publish_15m_30m_2h_snapshots_read_origo_spot_trades_with_shared_query(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_data_source_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
) -> None:
    partition_key = '2024-01-01'
    uploaded: dict[str, object] = {}
    captured_query: dict[str, object] = {}

    result = materialize_binance_spot_data_source_assets(partition_key=partition_key)
    assert result.success

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

    monkeypatch.setenv('HF_TOKEN', 'test-token')
    monkeypatch.setenv('HUGGINGFACE_DATASET_REPO_ID', 'test/not-used')
    monkeypatch.setattr(publish_helper_module, 'HfApi', RecordingHfApi)

    for case in KLINE_CASES:
        (
            cadence_label,
            resolution_label,
            kline_size,
            module_path,
            asset_name,
            _sensor_name,
            expected_repo_id,
            file_prefix,
        ) = case
        uploaded.clear()
        captured_query.clear()

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

            return _origo_trades_kline_dataframe(
                query_origo,
                kline_size=kline_size,
                database_name=database_name,
                table_name=table_name,
                start_date_limit=start_date_limit,
                end_date_limit=end_date_limit,
            )

        monkeypatch.setattr(
            publish_helper_module,
            'get_binance_spot_klines',
            recording_get_binance_spot_klines,
        )

        publish_module = importlib.import_module(module_path)
        publish_result = materialize(
            [getattr(publish_module, asset_name)],
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
        assert uploaded['repo_id'] == expected_repo_id
        assert uploaded['upload_repo_id'] == expected_repo_id
        assert uploaded['repo_type'] == 'dataset'
        assert uploaded['upload_repo_type'] == 'dataset'
        assert uploaded['exist_ok'] is True
        assert metadata['export_end_date'] == partition_key
        assert metadata['row_count'] == parquet.height
        assert parquet.columns == KLINE_EXPORT_COLUMNS
        assert captured_query == {
            'kline_size': kline_size,
            'start_date_limit': '2020-01-01 00:00:00',
            'end_date_limit': '2024-01-02 00:00:00',
            'table_name': 'binance_daily_spot_trades',
            'database_name': 'origo',
            'include_quantiles': False,
        }
        assert (
            uploaded['commit_message']
            == f'Add BTCUSDT {cadence_label} klines snapshot through 2024-01-01'
        )
        assert uploaded['delete_patterns'] == [f'{file_prefix}*.parquet']
        assert metadata['file_name'] == f'{file_prefix}20240101.parquet'
        assert f'{resolution_label} resolution' in readme
        assert 'origo.binance_daily_spot_trades' in readme
