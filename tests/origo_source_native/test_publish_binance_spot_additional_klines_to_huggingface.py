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
        'origo.assets.publish_binance_spot_15m_klines_to_huggingface',
        'publish_binance_spot_15m_klines_to_huggingface',
        'publish_binance_spot_15m_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_15m_klines',
        'btcusdt_15m_kline_20200101_to_',
    ),
    (
        '30m',
        '30-minute',
        1800,
        'origo.assets.publish_binance_spot_30m_klines_to_huggingface',
        'publish_binance_spot_30m_klines_to_huggingface',
        'publish_binance_spot_30m_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_30m_klines',
        'btcusdt_30m_kline_20200101_to_',
    ),
    (
        '2h',
        '2-hour',
        7200,
        'origo.assets.publish_binance_spot_2h_klines_to_huggingface',
        'publish_binance_spot_2h_klines_to_huggingface',
        'publish_binance_spot_2h_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_2h_klines',
        'btcusdt_2h_kline_20200101_to_',
    ),
]


def _origo_projection_kline_dataframe(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    *,
    kline_size_seconds: int,
    database_name: str,
    table_name: str,
    start_date_limit: str,
    end_date_limit: str,
) -> pl.DataFrame:
    rows = query_origo(
        f"""
        SELECT
            kline_datetime AS datetime,
            argMin(source_open, source_datetime) AS open,
            max(source_high) AS high,
            min(source_low) AS low,
            argMax(source_close, source_datetime) AS close,
            round(sum(source_no_of_trades * source_mean) / sum(source_no_of_trades), 5) AS mean,
            round(
                sqrt(
                    greatest(
                        sum(source_no_of_trades * ((source_std * source_std) + (source_mean * source_mean))) / sum(source_no_of_trades)
                        - pow(sum(source_no_of_trades * source_mean) / sum(source_no_of_trades), 2),
                        0
                    )
                ),
                6
            ) AS std,
            round(sumKahan(source_volume), 9) AS volume,
            sum(source_no_of_trades * source_maker_ratio) / sum(source_no_of_trades) AS maker_ratio,
            sum(source_no_of_trades) AS no_of_trades,
            argMin(source_open_liquidity, source_datetime) AS open_liquidity,
            max(source_high_liquidity) AS high_liquidity,
            min(source_low_liquidity) AS low_liquidity,
            argMax(source_close_liquidity, source_datetime) AS close_liquidity,
            round(sum(source_liquidity_sum), 1) AS liquidity_sum,
            sumKahan(source_maker_volume) AS maker_volume,
            round(sum(source_maker_liquidity), 1) AS maker_liquidity
        FROM (
            SELECT
                datetime AS source_datetime,
                open AS source_open,
                high AS source_high,
                low AS source_low,
                close AS source_close,
                mean AS source_mean,
                std AS source_std,
                volume AS source_volume,
                maker_ratio AS source_maker_ratio,
                no_of_trades AS source_no_of_trades,
                open_liquidity AS source_open_liquidity,
                high_liquidity AS source_high_liquidity,
                low_liquidity AS source_low_liquidity,
                close_liquidity AS source_close_liquidity,
                liquidity_sum AS source_liquidity_sum,
                maker_volume AS source_maker_volume,
                maker_liquidity AS source_maker_liquidity,
                toDateTime({kline_size_seconds} * intDiv(toUnixTimestamp(datetime), {kline_size_seconds})) AS kline_datetime
            FROM {database_name}.{table_name}
            WHERE datetime >= toDateTime('{start_date_limit}')
              AND datetime < toDateTime('{end_date_limit}')
        )
        GROUP BY kline_datetime
        ORDER BY kline_datetime
        """
    )
    return pl.DataFrame(rows, schema=KLINE_EXPORT_COLUMNS, orient='row')


def test_publish_time_kline_sensors_target_origo_spot_kline_materialization(
    origo_definitions_module: object,
) -> None:
    for case in KLINE_CASES:
        sensor_name = case[5]
        sensor_def = getattr(origo_definitions_module, sensor_name)

        assert sensor_def.asset_key == AssetKey('refresh_binance_spot_klines_origo')


def test_publish_time_kline_snapshots_read_origo_spot_klines_projection(
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
        'origo.utils.publish_binance_spot_kline_snapshot_to_huggingface'
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

        def recording_get_binance_spot_klines_from_1m_projection(
            **kwargs: object,
        ) -> pl.DataFrame:
            captured_query.update(kwargs)
            database_name = kwargs.get('database_name')
            table_name = kwargs.get('table_name')
            kline_size_seconds = kwargs.get('kline_size_seconds')
            start_date_limit = kwargs.get('start_date_limit')
            end_date_limit = kwargs.get('end_date_limit')

            assert database_name == ORIGO_DATABASE
            assert table_name == 'binance_spot_klines'
            assert isinstance(kline_size_seconds, int)
            assert isinstance(start_date_limit, str)
            assert isinstance(end_date_limit, str)

            return _origo_projection_kline_dataframe(
                query_origo,
                kline_size_seconds=kline_size_seconds,
                database_name=database_name,
                table_name=table_name,
                start_date_limit=start_date_limit,
                end_date_limit=end_date_limit,
            )

        monkeypatch.setattr(
            publish_helper_module,
            '_get_binance_spot_klines_from_1m_projection',
            recording_get_binance_spot_klines_from_1m_projection,
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
            'kline_size_seconds': kline_size,
            'start_date_limit': '2020-01-01 00:00:00',
            'end_date_limit': '2024-01-02 00:00:00',
            'table_name': 'binance_spot_klines',
            'database_name': 'origo',
        }
        assert (
            uploaded['commit_message']
            == f'Add BTCUSDT {cadence_label} klines snapshot through 2024-01-01'
        )
        assert uploaded['delete_patterns'] == [f'{file_prefix}*.parquet']
        assert metadata['file_name'] == f'{file_prefix}20240101.parquet'
        assert f'{resolution_label} resolution' in readme
        assert 'origo.binance_spot_klines' in readme
