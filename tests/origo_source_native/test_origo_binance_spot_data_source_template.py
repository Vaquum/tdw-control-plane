from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .helpers import BINANCE_SPOT_DATASET_SOURCE, ORIGO_DATABASE

TDW_CONTRACT_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / 'fixtures' / 'tdw' / 'binance_spot_1m_contract.json'
)
TDW_KLINE_COLUMN_TYPES = [
    'DateTime',
    *(['Float64'] * 10),
    'UInt64',
    *(['Float64'] * 7),
]
TDW_ALIGNED_COLUMN_TYPES = ['LowCardinality(String)', *TDW_KLINE_COLUMN_TYPES]


def _table_metadata(query_origo, table_name: str) -> tuple[str, str, str]:
    rows = query_origo(
        f"""
        SELECT engine, partition_key, sorting_key
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name = '{table_name}'
        """
    )

    assert len(rows) == 1
    engine, partition_key, sorting_key = rows[0]
    return str(engine), str(partition_key), str(sorting_key)


def _load_tdw_contract_fixture() -> dict[str, Any]:
    return json.loads(TDW_CONTRACT_FIXTURE_PATH.read_text())


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return value


def _rows_to_dicts(columns: list[str], rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    return [
        {column: _normalize_value(value) for column, value in zip(columns, row, strict=True)}
        for row in rows
    ]


def test_binance_spot_klines_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['KLINES_TABLE_NAME'] == 'binance_spot_klines'


def test_aligned_1m_exchange_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['ALIGNED_TABLE_NAME'] == 'aligned_1m_exchange'


def test_daily_binance_spot_pipeline_schedule_targets_binance_spot_data_source_job(
    origo_definitions_module,
) -> None:
    schedule_def = origo_definitions_module.daily_binance_spot_pipeline_schedule
    job_def = origo_definitions_module.defs.get_job_def('refresh_binance_spot_data_source_job')
    node_names = set(job_def.graph.node_dict.keys())

    assert not hasattr(origo_definitions_module, 'daily_pipeline_schedule')
    assert not hasattr(origo_definitions_module, 'daily_spot_pipeline_schedule')
    assert schedule_def.job.name == 'refresh_binance_spot_data_source_job'
    assert node_names >= {
        'insert_daily_binance_spot_trades_to_origo',
        'refresh_binance_spot_klines_origo',
        'refresh_aligned_1m_exchange_from_binance_spot_origo',
    }


def test_binance_spot_klines_schema_matches_tdw_contract_fixture(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()

    result = materialize_binance_spot_data_source_assets(partition_key=fixture['fixture_day'])
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == fixture['columns']
    assert [type_name for _, type_name, *_ in rows] == TDW_KLINE_COLUMN_TYPES
    assert _table_metadata(query_origo, origo_assets['KLINES_TABLE_NAME']) == (
        'MergeTree',
        'toYYYYMM(datetime)',
        'datetime',
    )


def test_binance_spot_klines_rows_match_tdw_contract_fixture_for_fixture_day(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()
    fixture_day = fixture['fixture_day']
    columns = fixture['columns']

    result = materialize_binance_spot_data_source_assets(partition_key=fixture_day)
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('{fixture_day}')
        ORDER BY datetime
        """
    )

    assert _rows_to_dicts(columns, rows) == fixture['binance_spot_klines_rows']


def test_aligned_1m_exchange_schema_matches_tdw_contract_fixture_plus_dataset_source(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()

    result = materialize_binance_spot_data_source_assets(partition_key=fixture['fixture_day'])
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        """
    )

    assert [name for name, *_ in rows] == ['dataset_source', *fixture['columns']]
    assert [type_name for _, type_name, *_ in rows] == TDW_ALIGNED_COLUMN_TYPES
    assert _table_metadata(query_origo, origo_assets['ALIGNED_TABLE_NAME']) == (
        'MergeTree',
        'toYYYYMM(datetime)',
        'dataset_source, datetime',
    )


def test_aligned_1m_exchange_rows_match_tdw_contract_fixture_plus_dataset_source(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    fixture = _load_tdw_contract_fixture()
    fixture_day = fixture['fixture_day']
    columns = ['dataset_source', *fixture['columns']]

    result = materialize_binance_spot_data_source_assets(partition_key=fixture_day)
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            {', '.join(columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_SPOT_DATASET_SOURCE"]}'
          AND toDate(datetime) = toDate('{fixture_day}')
        ORDER BY datetime
        """
    )

    assert _rows_to_dicts(columns, rows) == fixture['aligned_1m_exchange_rows']


def test_same_partition_rerun_is_idempotent_across_single_source_and_aligned(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    kline_columns = _load_tdw_contract_fixture()['columns']
    aligned_columns = ['dataset_source', *kline_columns]

    first = materialize_binance_spot_data_source_assets(partition_key='2024-01-02')
    first_kline_rows = query_origo(
        f"""
        SELECT
            {', '.join(kline_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2024-01-02')
        ORDER BY datetime
        """
    )
    first_aligned_rows = query_origo(
        f"""
        SELECT
            {', '.join(aligned_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_SPOT_DATASET_SOURCE"]}'
          AND toDate(datetime) = toDate('2024-01-02')
        ORDER BY datetime
        """
    )

    second = materialize_binance_spot_data_source_assets(partition_key='2024-01-02')
    second_kline_rows = query_origo(
        f"""
        SELECT
            {', '.join(kline_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2024-01-02')
        ORDER BY datetime
        """
    )
    second_aligned_rows = query_origo(
        f"""
        SELECT
            {', '.join(aligned_columns)}
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_SPOT_DATASET_SOURCE"]}'
          AND toDate(datetime) = toDate('2024-01-02')
        ORDER BY datetime
        """
    )

    assert first.success
    assert second.success
    assert first_kline_rows == second_kline_rows
    assert first_aligned_rows == second_aligned_rows
    assert len(second_kline_rows) == 1
    assert len(second_aligned_rows) == 1
    assert second_aligned_rows[0][0] == BINANCE_SPOT_DATASET_SOURCE


def test_refresh_assets_declare_immediate_dependencies(origo_assets: dict[str, object]) -> None:
    kline_asset = origo_assets['refresh_binance_spot_klines_origo']
    aligned_asset = origo_assets['refresh_aligned_1m_exchange_from_binance_spot_origo']

    kline_deps = kline_asset.asset_deps[kline_asset.key]
    aligned_deps = aligned_asset.asset_deps[aligned_asset.key]

    assert kline_deps == {
        origo_assets['create_binance_spot_klines_table_origo'].key,
        origo_assets['insert_daily_binance_spot_trades_to_origo'].key,
    }
    assert aligned_deps == {
        origo_assets['create_aligned_1m_exchange_table_origo'].key,
        origo_assets['create_binance_spot_klines_table_origo'].key,
        origo_assets['refresh_binance_spot_klines_origo'].key,
    }
