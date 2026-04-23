from __future__ import annotations

from pathlib import Path

from .helpers import (
    ALIGNED_SCHEMA_COLUMNS,
    KLINE_SCHEMA_COLUMNS,
    ORIGO_DATABASE,
    load_expected_aligned_1m_exchange_rows,
    load_expected_binance_spot_kline_rows,
)


def test_binance_spot_klines_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['KLINES_TABLE_NAME'] == 'binance_spot_klines'


def test_aligned_1m_exchange_table_name_contract(origo_assets: dict[str, object]) -> None:
    assert origo_assets['ALIGNED_TABLE_NAME'] == 'aligned_1m_exchange'


def test_daily_pipeline_schedule_targets_binance_spot_data_source_job(
) -> None:
    definitions_text = (
        Path(__file__).resolve().parents[2] / 'tdw_control_plane' / 'definitions.py'
    ).read_text(encoding='utf-8')

    assert "name=\"refresh_binance_spot_data_source_job\"" in definitions_text
    assert 'selection=[' in definitions_text
    assert '"insert_daily_binance_spot_trades_to_origo"' in definitions_text
    assert '"refresh_binance_spot_klines_origo"' in definitions_text
    assert '"refresh_aligned_1m_exchange_from_binance_spot_origo"' in definitions_text
    assert 'job=refresh_binance_spot_data_source_job' in definitions_text
    assert 'job=insert_daily_binance_spot_trades_to_origo_job' not in definitions_text


def test_binance_spot_klines_schema_matches_exchange_contract(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_data_source_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        """
    )

    assert [(name, type_name) for name, type_name, *_ in rows] == KLINE_SCHEMA_COLUMNS


def test_binance_spot_klines_exact_rows_match_expected(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_data_source_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            quote_asset_volume,
            number_of_trades,
            taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume,
            ignore
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE toDate(fromUnixTimestamp64Milli(open_time)) = toDate('2024-01-01')
        ORDER BY open_time
        """
    )

    assert rows == load_expected_binance_spot_kline_rows('2024-01-01')


def test_aligned_1m_exchange_schema_adds_dataset_source_column(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_data_source_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        DESCRIBE TABLE {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        """
    )

    assert [(name, type_name) for name, type_name, *_ in rows] == ALIGNED_SCHEMA_COLUMNS


def test_aligned_1m_exchange_rows_from_binance_spot_match_expected(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_binance_spot_data_source_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            dataset_source,
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            quote_asset_volume,
            number_of_trades,
            taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume,
            ignore
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_SPOT_DATASET_SOURCE"]}'
          AND toDate(fromUnixTimestamp64Milli(open_time)) = toDate('2024-01-01')
        ORDER BY open_time
        """
    )

    assert rows == load_expected_aligned_1m_exchange_rows('2024-01-01')


def test_same_partition_rerun_is_idempotent_across_single_source_and_aligned(
    materialize_binance_spot_data_source_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    first = materialize_binance_spot_data_source_assets(partition_key='2024-01-02')
    second = materialize_binance_spot_data_source_assets(partition_key='2024-01-02')

    assert first.success
    assert second.success

    kline_rows = query_origo(
        f"""
        SELECT
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            quote_asset_volume,
            number_of_trades,
            taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume,
            ignore
        FROM {ORIGO_DATABASE}.{origo_assets['KLINES_TABLE_NAME']}
        WHERE toDate(fromUnixTimestamp64Milli(open_time)) = toDate('2024-01-02')
        ORDER BY open_time
        """
    )
    aligned_rows = query_origo(
        f"""
        SELECT
            dataset_source,
            open_time,
            open,
            high,
            low,
            close,
            volume,
            close_time,
            quote_asset_volume,
            number_of_trades,
            taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume,
            ignore
        FROM {ORIGO_DATABASE}.{origo_assets['ALIGNED_TABLE_NAME']}
        WHERE dataset_source = '{origo_assets["BINANCE_SPOT_DATASET_SOURCE"]}'
          AND toDate(fromUnixTimestamp64Milli(open_time)) = toDate('2024-01-02')
        ORDER BY open_time
        """
    )

    assert kline_rows == load_expected_binance_spot_kline_rows('2024-01-02')
    assert aligned_rows == load_expected_aligned_1m_exchange_rows('2024-01-02')
    assert len(kline_rows) == 1
    assert len(aligned_rows) == 1


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
