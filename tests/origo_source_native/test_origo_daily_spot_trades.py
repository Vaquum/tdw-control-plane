from __future__ import annotations

import pytest
from dagster import AssetKey, materialize

from .helpers import ORIGO_DATABASE, load_expected_trade_rows


def test_create_origo_database_is_idempotent(
    origo_test_env: dict[str, str],
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    first = materialize([origo_assets['create_origo_database']])
    second = materialize([origo_assets['create_origo_database']])

    assert first.success
    assert second.success

    result = query_origo(
        f"""
        SELECT count()
        FROM system.databases
        WHERE name = '{ORIGO_DATABASE}'
        """
    )
    assert result == [(1,)]


def test_create_binance_daily_spot_trades_table_is_idempotent(
    origo_test_env: dict[str, str],
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    first = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_daily_spot_trades_table_origo'],
        ]
    )
    second = materialize(
        [
            origo_assets['create_origo_database'],
            origo_assets['create_binance_daily_spot_trades_table_origo'],
        ]
    )

    assert first.success
    assert second.success

    tables = query_origo(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name IN (
            '{origo_assets["RAW_TABLE_NAME"]}',
            '{origo_assets["LEDGER_TABLE_NAME"]}'
          )
        ORDER BY name
        """
    )
    assert {table_name for (table_name,) in tables} == {
        origo_assets['LEDGER_TABLE_NAME'],
        origo_assets['RAW_TABLE_NAME'],
    }


def test_dagster_asset_dependencies_are_declared(
    origo_assets: dict[str, object],
) -> None:
    table_asset = origo_assets['create_binance_daily_spot_trades_table_origo']
    ingest_asset = origo_assets['insert_daily_binance_spot_trades_to_origo']

    table_deps = table_asset.asset_deps[  # type: ignore[index]
        AssetKey('create_binance_daily_spot_trades_table_origo')
    ]
    ingest_deps = ingest_asset.asset_deps[  # type: ignore[index]
        AssetKey('insert_daily_binance_spot_trades_to_origo')
    ]

    assert table_deps == {AssetKey('create_origo_database')}
    assert ingest_deps == {AssetKey('create_binance_daily_spot_trades_table_origo')}


def test_insert_daily_spot_trades_exact_rows_match_source(
    materialize_origo_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_origo_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            is_best_match,
            datetime
        FROM {ORIGO_DATABASE}.{origo_assets['RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2024-01-01')
        ORDER BY trade_id
        """
    )
    assert rows == load_expected_trade_rows('2024-01-01')


def test_reingest_same_day_preserves_exact_final_state(
    materialize_origo_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    first = materialize_origo_assets(partition_key='2024-01-02')
    second = materialize_origo_assets(partition_key='2024-01-02')

    assert first.success
    assert second.success

    rows = query_origo(
        f"""
        SELECT
            trade_id,
            price,
            quantity,
            quote_quantity,
            timestamp,
            is_buyer_maker,
            is_best_match,
            datetime
        FROM {ORIGO_DATABASE}.{origo_assets['RAW_TABLE_NAME']}
        WHERE toDate(datetime) = toDate('2024-01-02')
        ORDER BY trade_id
        """
    )
    assert rows == load_expected_trade_rows('2024-01-02')


def test_missing_clickhouse_password_fails_loud(
    monkeypatch,
    binance_daily_base_url: str,
    clickhouse_settings: dict[str, str],
    origo_assets: dict[str, object],
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', clickhouse_settings['CLICKHOUSE_HOST'])
    monkeypatch.setenv('CLICKHOUSE_PORT', clickhouse_settings['CLICKHOUSE_PORT'])
    monkeypatch.setenv('CLICKHOUSE_USER', clickhouse_settings['CLICKHOUSE_USER'])
    monkeypatch.setenv('CLICKHOUSE_DATABASE', clickhouse_settings['CLICKHOUSE_DATABASE'])
    monkeypatch.setenv('BINANCE_SPOT_DAILY_TRADES_BASE_URL', binance_daily_base_url)
    monkeypatch.delenv('CLICKHOUSE_PASSWORD', raising=False)

    with pytest.raises(RuntimeError, match='CLICKHOUSE_PASSWORD environment variable must be set\\.'):
        materialize(
            [
                origo_assets['create_origo_database'],
                origo_assets['create_binance_daily_spot_trades_table_origo'],
                origo_assets['insert_daily_binance_spot_trades_to_origo'],
            ],
            partition_key='2024-01-01',
        )
