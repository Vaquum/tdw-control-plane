from __future__ import annotations

import importlib
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from dagster import AssetKey, materialize

from .helpers import ORIGO_DATABASE

DOLLAR_KLINE_EXPORT_COLUMNS = [
    'start_datetime',
    'end_datetime',
    'dollar_bar_id',
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

DOLLAR_KLINE_CASES = [
    (
        '15M',
        '15M-dollar',
        15_000_000.0,
        'tdw_control_plane.assets.publish_binance_spot_15m_dollar_klines_to_huggingface',
        'publish_binance_spot_15m_dollar_klines_to_huggingface',
        'publish_binance_spot_15m_dollar_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_15m_dollar_klines',
        'btcusdt_15m_dollar_kline_20200101_to_',
    ),
    (
        '30M',
        '30M-dollar',
        30_000_000.0,
        'tdw_control_plane.assets.publish_binance_spot_30m_dollar_klines_to_huggingface',
        'publish_binance_spot_30m_dollar_klines_to_huggingface',
        'publish_binance_spot_30m_dollar_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_30m_dollar_klines',
        'btcusdt_30m_dollar_kline_20200101_to_',
    ),
    (
        '60M',
        '60M-dollar',
        60_000_000.0,
        'tdw_control_plane.assets.publish_binance_spot_60m_dollar_klines_to_huggingface',
        'publish_binance_spot_60m_dollar_klines_to_huggingface',
        'publish_binance_spot_60m_dollar_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_60m_dollar_klines',
        'btcusdt_60m_dollar_kline_20200101_to_',
    ),
    (
        '120M',
        '120M-dollar',
        120_000_000.0,
        'tdw_control_plane.assets.publish_binance_spot_120m_dollar_klines_to_huggingface',
        'publish_binance_spot_120m_dollar_klines_to_huggingface',
        'publish_binance_spot_120m_dollar_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_120m_dollar_klines',
        'btcusdt_120m_dollar_kline_20200101_to_',
    ),
    (
        '240M',
        '240M-dollar',
        240_000_000.0,
        'tdw_control_plane.assets.publish_binance_spot_240m_dollar_klines_to_huggingface',
        'publish_binance_spot_240m_dollar_klines_to_huggingface',
        'publish_binance_spot_240m_dollar_klines_to_huggingface_sensor',
        'vaquum/binance_btcusdt_240m_dollar_klines',
        'btcusdt_240m_dollar_kline_20200101_to_',
    ),
]


def test_dollar_kline_query_uses_arrow_parameters_and_configured_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'test-password')
    publish_helper_module = importlib.import_module(
        'tdw_control_plane.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface'
    )
    create_origo_database_module = importlib.import_module(
        'tdw_control_plane.assets.create_origo_database'
    )
    recorded: dict[str, object] = {}

    row = {
        'start_datetime': datetime(2024, 1, 1, 0, 0, 0),
        'end_datetime': datetime(2024, 1, 1, 0, 0, 1),
        'dollar_bar_id': 0,
        'open': 42000.1,
        'high': 42010.0,
        'low': 42000.1,
        'close': 42005.5,
        'mean': 42005.2,
        'std': 4.047221,
        'volume': 0.0045,
        'maker_ratio': 0.5,
        'no_of_trades': 3,
        'open_liquidity': 42.0,
        'high_liquidity': 84.0,
        'low_liquidity': 42.0,
        'close_liquidity': 63.0,
        'liquidity_sum': 189.0,
        'maker_volume': 0.0025,
        'maker_liquidity': 105.0,
    }

    class RecordingArrowClient:
        def query_arrow(
            self,
            query: str,
            parameters: dict[str, object] | None = None,
        ) -> object:
            recorded['query'] = query
            recorded['parameters'] = parameters
            return pl.DataFrame([row]).to_arrow()

        def close(self) -> None:
            recorded['closed'] = True

    settings = create_origo_database_module.ClickHouseSettings(
        host='127.0.0.1',
        port=9000,
        user='default',
        password='test-password',
        database='origo_custom',
    )
    monkeypatch.setattr(publish_helper_module, 'get_clickhouse_settings', lambda: settings)
    monkeypatch.setattr(
        publish_helper_module,
        '_make_clickhouse_arrow_client',
        lambda _settings: RecordingArrowClient(),
    )

    data = publish_helper_module._get_binance_spot_dollar_klines(
        dollar_size=15_000_000.0,
        start_date_limit='2024-01-01',
        end_date_limit='2024-01-02T00:00:00',
        table_name='binance_spot_dollar_klines',
        database_name=settings.database,
    )

    assert data.height == 1
    assert recorded['closed'] is True
    assert 'FROM origo_custom.binance_spot_dollar_klines' in str(recorded['query'])
    assert '2024-01-01' not in str(recorded['query'])
    assert recorded['parameters'] == {
        'base_bar_count': 15,
        'start_dt': '2024-01-01 00:00:00',
        'end_dt': '2024-01-02 00:00:00',
    }


def test_dollar_kline_query_rejects_unsafe_sql_inputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'test-password')
    publish_helper_module = importlib.import_module(
        'tdw_control_plane.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface'
    )

    with pytest.raises(ValueError, match='Invalid ClickHouse database name'):
        publish_helper_module._get_binance_spot_dollar_klines(
            dollar_size=1_000_000.0,
            start_date_limit='2024-01-01',
            end_date_limit='2024-01-02',
            table_name='binance_spot_dollar_klines',
            database_name='origo; DROP TABLE x',
        )

    with pytest.raises(ValueError, match='start_date_limit must match one of'):
        publish_helper_module._get_binance_spot_dollar_klines(
            dollar_size=1_000_000.0,
            start_date_limit='2024/01/01',
            end_date_limit='2024-01-02',
            table_name='binance_spot_dollar_klines',
            database_name='origo',
        )


def _origo_dollar_klines_dataframe(
    query_origo: Callable[[str], list[tuple[object, ...]]],
    *,
    dollar_size: float,
    database_name: str,
    table_name: str,
    start_date_limit: str,
    end_date_limit: str,
) -> pl.DataFrame:
    base_bar_count = int(dollar_size / 1_000_000.0)
    rows = query_origo(
        f"""
        SELECT
            min(source_start_datetime) AS start_datetime,
            max(source_end_datetime) AS end_datetime,
            target_dollar_bar_id AS dollar_bar_id,
            argMin(source_open, tuple(source_start_datetime, source_dollar_bar_id)) AS open,
            max(source_high) AS high,
            min(source_low) AS low,
            argMax(source_close, tuple(source_end_datetime, source_dollar_bar_id)) AS close,
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
            argMin(source_open_liquidity, tuple(source_start_datetime, source_dollar_bar_id)) AS open_liquidity,
            max(source_high_liquidity) AS high_liquidity,
            min(source_low_liquidity) AS low_liquidity,
            argMax(source_close_liquidity, tuple(source_end_datetime, source_dollar_bar_id)) AS close_liquidity,
            round(sum(source_liquidity_sum), 1) AS liquidity_sum,
            sumKahan(source_maker_volume) AS maker_volume,
            round(sum(source_maker_liquidity), 1) AS maker_liquidity
        FROM (
            SELECT
                start_datetime AS source_start_datetime,
                end_datetime AS source_end_datetime,
                dollar_bar_id AS source_dollar_bar_id,
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
                toDate(start_datetime) AS bar_date,
                intDiv(dollar_bar_id, {base_bar_count}) AS target_dollar_bar_id
            FROM {database_name}.{table_name}
            WHERE start_datetime >= toDateTime('{start_date_limit}')
              AND start_datetime < toDateTime('{end_date_limit}')
        )
        GROUP BY bar_date, target_dollar_bar_id
        ORDER BY start_datetime ASC, dollar_bar_id ASC
        """
    )
    return pl.DataFrame(rows, schema=DOLLAR_KLINE_EXPORT_COLUMNS, orient='row')


def test_publish_dollar_kline_sensors_target_origo_dollar_kline_materialization(
    origo_definitions_module: object,
) -> None:
    for case in DOLLAR_KLINE_CASES:
        sensor_name = case[5]
        sensor_def = getattr(origo_definitions_module, sensor_name)

        assert sensor_def.asset_key == AssetKey('refresh_binance_spot_dollar_klines_origo')


def test_publish_dollar_kline_snapshots_read_origo_dollar_klines_with_shared_helper(
    monkeypatch: pytest.MonkeyPatch,
    materialize_binance_spot_dollar_klines_assets: Callable[..., object],
    query_origo: Callable[[str], list[tuple[object, ...]]],
) -> None:
    partition_key = '2024-01-01'
    uploaded: dict[str, object] = {}
    captured_query: dict[str, object] = {}

    result = materialize_binance_spot_dollar_klines_assets(partition_key=partition_key)
    assert result.success

    publish_helper_module = importlib.import_module(
        'tdw_control_plane.utils.publish_binance_spot_dollar_kline_snapshot_to_huggingface'
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

    for case in DOLLAR_KLINE_CASES:
        (
            size_label,
            resolution_label,
            dollar_size,
            module_path,
            asset_name,
            _sensor_name,
            expected_repo_id,
            file_prefix,
        ) = case
        uploaded.clear()
        captured_query.clear()

        def recording_get_binance_spot_dollar_klines(**kwargs: object) -> pl.DataFrame:
            captured_query.update(kwargs)
            database_name = kwargs.get('database_name')
            table_name = kwargs.get('table_name')
            start_date_limit = kwargs.get('start_date_limit')
            end_date_limit = kwargs.get('end_date_limit')
            actual_dollar_size = kwargs.get('dollar_size')

            assert database_name == ORIGO_DATABASE
            assert table_name == 'binance_spot_dollar_klines'
            assert actual_dollar_size == dollar_size
            assert isinstance(start_date_limit, str)
            assert isinstance(end_date_limit, str)

            return _origo_dollar_klines_dataframe(
                query_origo,
                dollar_size=dollar_size,
                database_name=database_name,
                table_name=table_name,
                start_date_limit=start_date_limit,
                end_date_limit=end_date_limit,
            )

        monkeypatch.setattr(
            publish_helper_module,
            '_get_binance_spot_dollar_klines',
            recording_get_binance_spot_dollar_klines,
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
        assert parquet.columns == DOLLAR_KLINE_EXPORT_COLUMNS
        assert captured_query == {
            'dollar_size': dollar_size,
            'start_date_limit': '2020-01-01 00:00:00',
            'end_date_limit': '2024-01-02 00:00:00',
            'table_name': 'binance_spot_dollar_klines',
            'database_name': 'origo',
        }
        assert (
            uploaded['commit_message']
            == f'Add BTCUSDT {size_label} dollar klines snapshot through 2024-01-01'
        )
        assert uploaded['delete_patterns'] == [f'{file_prefix}*.parquet']
        assert metadata['file_name'] == f'{file_prefix}20240101.parquet'
        assert f'{resolution_label} dollar bars' in readme
        assert 'origo.binance_spot_dollar_klines' in readme
