from __future__ import annotations

from collections.abc import Callable, Set as AbstractSet
from dataclasses import dataclass
from datetime import date, timedelta

import requests
from dagster import RunRequest, SkipReason

from origo.assets.create_origo_database import ClickHouseClientProtocol

DAILY_GAP_REPAIR_LOOKBACK_DAYS = 14
MAX_GAP_REPAIR_RUNS_PER_TICK = 3
ARCHIVE_PROBE_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class DailyGapRepairSpec:
    market: str
    ledger_table: str
    earliest_partition: date
    get_base_url: Callable[[], str]


def source_filename(day: date) -> str:
    return f'BTCUSDT-trades-{day.isoformat()}.zip'


def archive_available(base_url: str, day: date) -> bool:
    checksum_url = f'{base_url}{source_filename(day)}.CHECKSUM'
    try:
        response = requests.get(checksum_url, timeout=ARCHIVE_PROBE_TIMEOUT_SECONDS)
    except requests.RequestException:
        return False
    try:
        return response.status_code == 200
    finally:
        response.close()


def repair_window(today: date, earliest_partition: date) -> tuple[date, date] | None:
    """[today - lookback, today - 2]; yesterday is left to the regular daily tick.

    Ending at today - 2 means the hourly repair never races the 04:00/10:00
    schedules over yesterday's partition: a missed yesterday becomes
    repairable the following day.
    """
    end = today - timedelta(days=2)
    start = max(today - timedelta(days=DAILY_GAP_REPAIR_LOOKBACK_DAYS), earliest_partition)
    if end < start:
        return None
    return start, end


def loaded_ledger_days(
    client: ClickHouseClientProtocol,
    database: str,
    ledger_table: str,
    start: date,
    end: date,
) -> set[date]:
    rows = client.execute(
        f"""
        SELECT DISTINCT source_date
        FROM {database}.{ledger_table}
        WHERE status = 'success'
          AND source_date >= toDate('{start.isoformat()}')
          AND source_date <= toDate('{end.isoformat()}')
        """
    )
    days: set[date] = set()
    for row in rows:
        value = row[0]
        if isinstance(value, date):
            days.add(value)
    return days


def repairable_gap_days(
    client: ClickHouseClientProtocol,
    database: str,
    spec: DailyGapRepairSpec,
    today: date,
    active_partition_days: AbstractSet[date],
) -> list[date]:
    """Ledger-absent days with an available archive and no in-flight run.

    ``active_partition_days`` are partitions with an in-progress run of the
    same job — including a regular daily tick still inside its op-retry
    backoff (RetryPolicy holds the run in STARTED for up to ~23h) — which
    must never be raced by a concurrent repair run of the non-atomic
    delete-then-insert assets.
    """
    window = repair_window(today, spec.earliest_partition)
    if window is None:
        return []
    start, end = window
    loaded = loaded_ledger_days(client, database, spec.ledger_table, start, end)
    base_url = spec.get_base_url()
    gaps: list[date] = []
    day = start
    while day <= end and len(gaps) < MAX_GAP_REPAIR_RUNS_PER_TICK:
        if day not in loaded and day not in active_partition_days and archive_available(base_url, day):
            gaps.append(day)
        day += timedelta(days=1)
    return gaps


def gap_repair_run_requests(
    client: ClickHouseClientProtocol,
    database: str,
    spec: DailyGapRepairSpec,
    today: date,
    active_partition_days: AbstractSet[date],
) -> list[RunRequest] | SkipReason:
    """One RunRequest per repairable gap day, keyed once per day per gap.

    The run key includes today's date so an unrepaired gap is re-requested
    at most once per day until its ledger row appears, without ever
    re-running a partition the same day it was already requested.
    """
    gaps = repairable_gap_days(client, database, spec, today, active_partition_days)
    if not gaps:
        return SkipReason(f'{spec.market}: no repairable daily gaps in lookback.')
    return [
        RunRequest(
            partition_key=day.isoformat(),
            run_key=f'daily_gap_repair:{spec.market}:{day.isoformat()}:{today.isoformat()}',
        )
        for day in gaps
    ]
