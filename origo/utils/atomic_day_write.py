from __future__ import annotations

from collections.abc import Sequence

from origo.assets.create_origo_database import ClickHouseClientProtocol

# The heavy client-side row insert of a full day (millions of rows over
# minutes) is the window in which a killed run leaves the live table with a
# partial day. This helper builds and count-verifies the day in a per-day
# staging table so the live table is never touched until the day is proven
# complete, then promotes it with an ATOMIC metadata operation:
#   1. synchronous DELETE of the day's old rows from the live month partition;
#   2. ALTER TABLE <staging> MOVE PARTITION ID '<yyyymm>' TO TABLE <main>.
# MOVE PARTITION is a single metadata part-move, not a multi-block
# INSERT..SELECT (which ClickHouse splits at max_insert_block_size and can
# leave partially committed on cancellation). It appends the staging month
# partition — which holds only the replacement day — into the live month,
# preserving the live month's other days. The only residual failure window is
# between the DELETE and the MOVE: a kill there leaves the day MISSING (never
# partial or duplicated) and no ledger 'success' row, which the daily
# gap-repair schedule then heals.
_STAGING_INSERT_MAX_EXECUTION_SECONDS = 900


def _staging_table_name(main_table: str, date_str: str) -> str:
    return f'{main_table}__staging_{date_str.replace("-", "")}'


def _month_partition_id(date_str: str) -> str:
    # toYYYYMM(datetime) partition id, e.g. '2024-01-01' -> '202401'.
    return date_str[:7].replace('-', '')


def _count_day(
    client: ClickHouseClientProtocol, database: str, table: str, date_str: str
) -> int:
    result = client.execute(
        f"""
        SELECT count()
        FROM {database}.{table}
        WHERE toDate(datetime) = toDate('{date_str}')
        """
    )
    return int(result[0][0])


def replace_day_via_staging(
    client: ClickHouseClientProtocol,
    database: str,
    main_table: str,
    date_str: str,
    columns: Sequence[str],
    rows: list[tuple[object, ...]],
) -> int:
    """Replace one day's rows in ``main_table`` through a staging table.

    Builds and count-verifies the day in ``<main_table>__staging_<yyyymmdd>``
    before touching the live table, then promotes it atomically with a
    synchronous day DELETE followed by ``MOVE PARTITION`` (metadata part-move,
    not a splittable INSERT..SELECT). Returns the live row count for the day,
    which the caller asserts against the source row count before writing the
    ledger. The staging table is always dropped, and a stale staging table
    from a crashed prior attempt is dropped before the rebuild.
    """
    staging = _staging_table_name(main_table, date_str)
    column_list = ', '.join(columns)
    month_partition_id = _month_partition_id(date_str)
    try:
        client.execute(f'DROP TABLE IF EXISTS {database}.{staging}')
        client.execute(f'CREATE TABLE {database}.{staging} AS {database}.{main_table}')
        client.execute(
            f'INSERT INTO {database}.{staging} ({column_list}) VALUES',
            rows,
            settings={'max_execution_time': _STAGING_INSERT_MAX_EXECUTION_SECONDS},
        )
        staged_count = _count_day(client, database, staging, date_str)
        if staged_count != len(rows):
            raise ValueError(
                f'Staging row count mismatch for {date_str}: '
                f'expected {len(rows)}, staged {staged_count}. Live table untouched.'
            )

        # Live table touched only from here, both metadata-level:
        # remove the old day, then atomically move in the staged day.
        client.execute(
            f"""
            ALTER TABLE {database}.{main_table}
            DELETE WHERE toDate(datetime) = toDate('{date_str}')
            """,
            settings={'mutations_sync': 2},
        )
        client.execute(
            f"ALTER TABLE {database}.{staging} "
            f"MOVE PARTITION ID '{month_partition_id}' TO TABLE {database}.{main_table}"
        )
        final_count = _count_day(client, database, main_table, date_str)
        if final_count != len(rows):
            raise ValueError(
                f'Live row count mismatch for {date_str} after promotion: '
                f'expected {len(rows)}, live {final_count}.'
            )
        return final_count
    finally:
        client.execute(f'DROP TABLE IF EXISTS {database}.{staging}')
