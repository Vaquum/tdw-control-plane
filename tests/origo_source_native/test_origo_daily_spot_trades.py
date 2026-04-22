from __future__ import annotations

import pytest
from dagster import AssetKey, materialize

from tdw_control_plane.assets.create_binance_trades_table_origo import (
    LEDGER_TABLE_NAME,
    RAW_TABLE_NAME,
    create_binance_daily_spot_trades_table_origo,
)
from tdw_control_plane.assets.create_origo_database import create_origo_database
from tdw_control_plane.assets.daily_trades_to_origo import (
    insert_daily_binance_spot_trades_to_origo,
)

from .helpers import ORIGO_DATABASE, load_expected_trade_rows


def test_create_origo_database_is_idempotent(
    origo_test_env: dict[str, str],
    query_origo,
) -> None:
    first = materialize([create_origo_database])
    second = materialize([create_origo_database])

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
) -> None:
    first = materialize([create_origo_database, create_binance_daily_spot_trades_table_origo])
    second = materialize([create_origo_database, create_binance_daily_spot_trades_table_origo])

    assert first.success
    assert second.success

    tables = query_origo(
        f"""
        SELECT name
        FROM system.tables
        WHERE database = '{ORIGO_DATABASE}'
          AND name IN ('{RAW_TABLE_NAME}', '{LEDGER_TABLE_NAME}')
        ORDER BY name
        """
    )
    assert {table_name for (table_name,) in tables} == {LEDGER_TABLE_NAME, RAW_TABLE_NAME}


def test_dagster_asset_dependencies_are_declared() -> None:
    table_deps = create_binance_daily_spot_trades_table_origo.asset_deps[  # type: ignore[index]
        AssetKey('create_binance_daily_spot_trades_table_origo')
    ]
    ingest_deps = insert_daily_binance_spot_trades_to_origo.asset_deps[  # type: ignore[index]
        AssetKey('insert_daily_binance_spot_trades_to_origo')
    ]

    assert table_deps == {AssetKey('create_origo_database')}
    assert ingest_deps == {AssetKey('create_binance_daily_spot_trades_table_origo')}


def test_insert_daily_spot_trades_exact_rows_match_source(
    materialize_origo_assets,
    query_origo,
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
        FROM {ORIGO_DATABASE}.{RAW_TABLE_NAME}
        WHERE toDate(datetime) = toDate('2024-01-01')
        ORDER BY trade_id
        """
    )
    assert rows == load_expected_trade_rows('2024-01-01')


def test_reingest_same_day_preserves_exact_final_state(
    materialize_origo_assets,
    query_origo,
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
        FROM {ORIGO_DATABASE}.{RAW_TABLE_NAME}
        WHERE toDate(datetime) = toDate('2024-01-02')
        ORDER BY trade_id
        """
    )
    assert rows == load_expected_trade_rows('2024-01-02')


def test_missing_clickhouse_password_fails_loud(
    monkeypatch,
    binance_daily_base_url: str,
) -> None:
    monkeypatch.setenv('CLICKHOUSE_HOST', '127.0.0.1')
    monkeypatch.setenv('CLICKHOUSE_PORT', '9000')
    monkeypatch.setenv('CLICKHOUSE_USER', 'default')
    monkeypatch.setenv('CLICKHOUSE_DATABASE', ORIGO_DATABASE)
    monkeypatch.setenv('BINANCE_SPOT_DAILY_TRADES_BASE_URL', binance_daily_base_url)
    monkeypatch.delenv('CLICKHOUSE_PASSWORD', raising=False)

    with pytest.raises(RuntimeError, match='CLICKHOUSE_PASSWORD environment variable must be set\\.'):
        materialize(
            [
                create_origo_database,
                create_binance_daily_spot_trades_table_origo,
                insert_daily_binance_spot_trades_to_origo,
            ],
            partition_key='2024-01-01',
        )
