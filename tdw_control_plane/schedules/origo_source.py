import os
from datetime import date, datetime, timedelta, timezone
from typing import cast

import requests
from clickhouse_driver import Client as ClickhouseClient
from dagster import RunRequest, ScheduleEvaluationContext, SkipReason


def _password() -> str:
    password = os.environ.get('CLICKHOUSE_PASSWORD')
    if not password:
        raise RuntimeError('CLICKHOUSE_PASSWORD environment variable must be set.')
    return password


def _database() -> str:
    return os.environ.get('CLICKHOUSE_DATABASE', 'origo')


def _clickhouse_client() -> ClickhouseClient:
    return ClickhouseClient(
        host=os.environ.get('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(os.environ.get('CLICKHOUSE_PORT', '9000')),
        user=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=_password(),
    )


def _identifier(value: str) -> str:
    if not value.replace('_', '').isalnum():
        raise ValueError(f'Invalid ClickHouse identifier: {value}')
    return value


def _rows(query: str, params: dict[str, object] | None = None) -> list[tuple[object, ...]]:
    client = _clickhouse_client()
    try:
        result: object = client.execute(query, params)
    finally:
        client.disconnect()

    if not isinstance(result, list):
        raise TypeError(f'Expected ClickHouse rows, got {type(result).__name__}')
    return cast(list[tuple[object, ...]], result)


def _date_value(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f'Expected date scalar from ClickHouse, got {type(value).__name__}')
    return value


def _table_exists(table_name: str) -> bool:
    rows = _rows(
        """
        SELECT count()
        FROM system.tables
        WHERE database = %(database)s
          AND name = %(table_name)s
        """,
        {'database': _database(), 'table_name': table_name},
    )
    return bool(rows[0][0])


def _latest_source_day(table_name: str, dataset_source: str) -> date | None:
    rows = _rows(
        f"""
        SELECT max(toDate(datetime))
        FROM {_identifier(_database())}.{_identifier(table_name)}
        WHERE dataset_source = %(dataset_source)s
        """,
        {'dataset_source': dataset_source},
    )
    return _date_value(rows[0][0])


def _existing_source_days(
    table_name: str,
    dataset_source: str,
    start_date: date,
    end_date: date,
) -> set[date]:
    rows = _rows(
        f"""
        SELECT DISTINCT toDate(datetime) AS day
        FROM {_identifier(_database())}.{_identifier(table_name)}
        WHERE dataset_source = %(dataset_source)s
          AND toDate(datetime) >= toDate(%(start_date)s)
          AND toDate(datetime) <= toDate(%(end_date)s)
        """,
        {
            'dataset_source': dataset_source,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
        },
    )
    return {day for row in rows if (day := _date_value(row[0])) is not None}


def _archive_available(file_url: str) -> bool:
    response = requests.get(f'{file_url}.CHECKSUM', timeout=30)
    return response.status_code == 200


def origo_source_schedule_requests(
    context: ScheduleEvaluationContext,
    *,
    table_name: str,
    dataset_source: str,
    run_key_prefix: str,
    file_url_prefix: str,
    max_gap_days: int,
    max_runs: int,
) -> list[RunRequest] | SkipReason:
    scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
    end_date = (scheduled_time - timedelta(days=1)).date()

    if not _table_exists(table_name):
        return SkipReason(f'{table_name} does not exist yet.')

    latest_day = _latest_source_day(table_name, dataset_source)
    if latest_day is not None and latest_day < end_date - timedelta(days=max_gap_days):
        return SkipReason(
            f'{dataset_source} aligned daily gap is larger than the automated backfill threshold; trigger a manual backfill.'
        )

    start_date = end_date - timedelta(days=max_gap_days - 1)
    existing_days = _existing_source_days(table_name, dataset_source, start_date, end_date)

    requests_to_run: list[RunRequest] = []
    current_day = start_date
    while current_day <= end_date and len(requests_to_run) < max_runs:
        if current_day not in existing_days:
            partition_key = current_day.isoformat()
            if _archive_available(f'{file_url_prefix}{partition_key}.zip'):
                requests_to_run.append(
                    RunRequest(
                        partition_key=partition_key,
                        run_key=f'{run_key_prefix}::{partition_key}',
                    )
                )
        current_day += timedelta(days=1)

    if not requests_to_run:
        return SkipReason(
            f'No available Binance daily archives were found for missing {dataset_source} aligned days.'
        )
    return requests_to_run
