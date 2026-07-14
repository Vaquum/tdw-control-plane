from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from origo.utils.atomic_day_write import _staging_table_name, replace_day_via_staging

DATABASE = 'origo'
MAIN_TABLE = 'binance_daily_spot_trades'
COLUMNS = ('trade_id', 'price', 'datetime')
DATE = '2024-01-01'
STAGING = _staging_table_name(MAIN_TABLE, DATE)


class _FakeClient:
    """Records executed statements; answers count() with scripted values."""

    def __init__(self, staging_count: int, live_count: int) -> None:
        self.statements: list[str] = []
        self._staging_count = staging_count
        self._live_count = live_count

    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: Mapping[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(query)
        if 'SELECT count()' in query:
            table = STAGING if STAGING in query else MAIN_TABLE
            return [(self._staging_count if table == STAGING else self._live_count,)]
        return []

    def disconnect(self) -> None:
        return None


def _rows(n: int) -> list[tuple[object, ...]]:
    return [(i, float(i), '2024-01-01 00:00:00') for i in range(n)]


def _touches_live_table(statements: Sequence[str]) -> bool:
    return any(
        f'{DATABASE}.{MAIN_TABLE}' in s and STAGING not in s and 'SELECT count()' not in s
        for s in statements
    )


def test_happy_path_swaps_and_returns_live_count() -> None:
    client = _FakeClient(staging_count=3, live_count=3)

    result = replace_day_via_staging(client, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(3))

    assert result == 3
    joined = '\n'.join(client.statements)
    assert f'CREATE TABLE {DATABASE}.{STAGING} AS {DATABASE}.{MAIN_TABLE}' in joined
    assert f'ALTER TABLE {DATABASE}.{MAIN_TABLE}' in joined  # delete happened
    assert f'INSERT INTO {DATABASE}.{MAIN_TABLE}' in joined
    assert f'SELECT trade_id, price, datetime FROM {DATABASE}.{STAGING}' in joined


def test_staging_mismatch_leaves_live_table_untouched() -> None:
    # Staging built with the wrong count -> raise before any live-table write.
    client = _FakeClient(staging_count=99, live_count=0)

    with pytest.raises(ValueError, match='Staging row count mismatch'):
        replace_day_via_staging(client, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(3))

    assert not _touches_live_table(client.statements)


def test_staging_table_dropped_on_success_and_failure() -> None:
    ok = _FakeClient(staging_count=2, live_count=2)
    replace_day_via_staging(ok, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(2))
    assert f'DROP TABLE IF EXISTS {DATABASE}.{STAGING}' in ok.statements[-1]

    bad = _FakeClient(staging_count=5, live_count=0)
    with pytest.raises(ValueError):
        replace_day_via_staging(bad, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(2))
    assert f'DROP TABLE IF EXISTS {DATABASE}.{STAGING}' in bad.statements[-1]


def test_staging_dropped_before_rebuild() -> None:
    client = _FakeClient(staging_count=1, live_count=1)
    replace_day_via_staging(client, DATABASE, MAIN_TABLE, DATE, COLUMNS, _rows(1))
    drop_idx = client.statements.index(f'DROP TABLE IF EXISTS {DATABASE}.{STAGING}')
    create_idx = next(
        i for i, s in enumerate(client.statements) if s.startswith(f'CREATE TABLE {DATABASE}.{STAGING}')
    )
    assert drop_idx < create_idx


def test_no_staging_table_remains_after_materialize(
    materialize_origo_assets: Any,
    query_origo: Any,
) -> None:
    materialize_origo_assets(partition_key='2024-01-01')

    remaining = query_origo(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{DATABASE}' AND name LIKE '%__staging_%'
        """
    )

    assert remaining[0][0] == 0
