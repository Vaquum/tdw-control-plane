from __future__ import annotations

from collections.abc import Sequence

from origo.assets.create_origo_database import ClickHouseClientProtocol

# The heavy client-side row insert of a full day (millions of rows over
# minutes) is the window in which a killed run leaves the live table with a
# partial day. This helper moves that insert into a per-day staging table so
# the live table is never touched until the day is fully built and
# count-verified; the live-table mutation is then a minimal server-side
# DELETE + INSERT ... SELECT that runs in seconds. It is atomic in practice
# (the vulnerable work happens off the live table), not a single engine-level
# atomic operation, which the monthly partitioning of these tables precludes
# at day granularity.
_MAIN_INSERT_MAX_EXECUTION_SECONDS = 900


def _staging_table_name(main_table: str, date_str: str) -> str:
    return f'{main_table}__staging_{date_str.replace("-", "")}'


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
    before touching the live table, then swaps it in with a minimal
    server-side DELETE + INSERT..SELECT. Returns the live row count for the
    day, which the caller asserts against the source row count before writing
    the ledger. The staging table is always dropped, and a stale staging
    table from a crashed prior attempt is dropped before the rebuild.
    """
    staging = _staging_table_name(main_table, date_str)
    column_list = ', '.join(columns)
    try:
        client.execute(f'DROP TABLE IF EXISTS {database}.{staging}')
        client.execute(f'CREATE TABLE {database}.{staging} AS {database}.{main_table}')
        client.execute(
            f'INSERT INTO {database}.{staging} ({column_list}) VALUES',
            rows,
            settings={'max_execution_time': _MAIN_INSERT_MAX_EXECUTION_SECONDS},
        )
        staged_count = _count_day(client, database, staging, date_str)
        if staged_count != len(rows):
            raise ValueError(
                f'Staging row count mismatch for {date_str}: '
                f'expected {len(rows)}, staged {staged_count}. Live table untouched.'
            )

        # Live table touched only from here: minimal, server-side, seconds.
        client.execute(
            f"""
            ALTER TABLE {database}.{main_table}
            DELETE WHERE toDate(datetime) = toDate('{date_str}')
            """,
            settings={'mutations_sync': 2},
        )
        client.execute(
            f'INSERT INTO {database}.{main_table} ({column_list}) '
            f'SELECT {column_list} FROM {database}.{staging}'
        )
        final_count = _count_day(client, database, main_table, date_str)
        if final_count != len(rows):
            raise ValueError(
                f'Live row count mismatch for {date_str} after swap: '
                f'expected {len(rows)}, live {final_count}.'
            )
        return final_count
    finally:
        client.execute(f'DROP TABLE IF EXISTS {database}.{staging}')
