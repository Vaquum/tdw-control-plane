from __future__ import annotations

from datetime import datetime

from .helpers import ORIGO_DATABASE, load_expected_ledger_payload


def test_ingestion_ledger_checksums_match_source(
    materialize_origo_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_origo_assets(partition_key='2024-01-01')
    assert result.success

    expected = load_expected_ledger_payload('2024-01-01')
    rows = query_origo(
        f"""
        SELECT source_file, zip_checksum, csv_checksum
        FROM {ORIGO_DATABASE}.{origo_assets['LEDGER_TABLE_NAME']}
        WHERE source_date = toDate('2024-01-01')
        """
    )

    assert rows == [
        (
            expected['source_file'],
            expected['zip_checksum'],
            expected['csv_checksum'],
        )
    ]


def test_ingestion_ledger_counts_match_source_and_insert(
    materialize_origo_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_origo_assets(partition_key='2024-01-01')
    assert result.success

    expected = load_expected_ledger_payload('2024-01-01')
    rows = query_origo(
        f"""
        SELECT source_row_count, inserted_row_count
        FROM {ORIGO_DATABASE}.{origo_assets['LEDGER_TABLE_NAME']}
        WHERE source_date = toDate('2024-01-01')
        """
    )

    assert rows == [
        (
            expected['source_row_count'],
            expected['source_row_count'],
        )
    ]


def test_ingestion_ledger_run_metadata_is_present_and_accurate(
    materialize_origo_assets,
    query_origo,
    origo_assets: dict[str, object],
) -> None:
    result = materialize_origo_assets(partition_key='2024-01-01')
    assert result.success

    rows = query_origo(
        f"""
        SELECT dagster_run_id, dagster_partition_key, loaded_at, status
        FROM {ORIGO_DATABASE}.{origo_assets['LEDGER_TABLE_NAME']}
        WHERE source_date = toDate('2024-01-01')
        """
    )

    assert len(rows) == 1
    dagster_run_id, dagster_partition_key, loaded_at, status = rows[0]
    assert dagster_run_id
    assert dagster_partition_key == '2024-01-01'
    assert isinstance(loaded_at, datetime)
    assert status == 'success'
