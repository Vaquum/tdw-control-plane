import fcntl
import hashlib
import io
import json
import os
import time
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
from dagster import (
    AssetExecutionContext,
    Config,
    RunConfig,
    RunRequest,
    StaticPartitionsDefinition,
    asset,
)

from origo.assets.build_bar_store_arrow import BarSeriesBuild, series_store_dir
from origo.assets.create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseClient,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from origo.assets.create_binance_spot_depth20_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH20_SNAPSHOTS_TABLE_NAME,
)
from origo.assets.create_binance_spot_depth200_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH200_SNAPSHOTS_TABLE_NAME,
)

DEPTH_SNAPSHOT_SERIES: tuple[str, ...] = ('depth20_snapshots', 'depth200_snapshots')
DEPTH_SNAPSHOT_PARTITIONS = StaticPartitionsDefinition(list(DEPTH_SNAPSHOT_SERIES))
DEPTH20_SOURCE_JOB_NAME = 'refresh_binance_spot_depth20_data_source_job'
DEPTH200_SOURCE_JOB_NAME = 'refresh_binance_spot_depth200_data_source_job'
LATEST_MANIFEST_NAME = 'latest.json'
VERSION_HEX = 16

BookLevel = tuple[float, float]


class DepthSnapshotStoreConfig(Config):
    dry_run: bool = False
    source_partition_key: str | None = None


@dataclass(frozen=True)
class DepthSnapshotSpec:
    series: str
    table_name: str
    depth: int


@dataclass(frozen=True)
class DepthSnapshotRow:
    ts: int
    source_timestamp_ms: int
    last_update_id: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(frozen=True)
class DepthSnapshotChunkPublish:
    status: str
    version: str | None
    chunk: str | None


_DEPTH_SNAPSHOT_SPECS: tuple[DepthSnapshotSpec, ...] = (
    DepthSnapshotSpec('depth20_snapshots', DEPTH20_SNAPSHOTS_TABLE_NAME, 20),
    DepthSnapshotSpec('depth200_snapshots', DEPTH200_SNAPSHOTS_TABLE_NAME, 200),
)

_SOURCE_JOB_TO_SERIES: dict[str, str] = {
    DEPTH20_SOURCE_JOB_NAME: 'depth20_snapshots',
    DEPTH200_SOURCE_JOB_NAME: 'depth200_snapshots',
}


def spec_for_depth_snapshot_series(series: str) -> DepthSnapshotSpec:
    for spec in _DEPTH_SNAPSHOT_SPECS:
        if spec.series == series:
            return spec
    raise ValueError(f'Unknown depth snapshot series: {series}')


def depth_snapshot_store_partition_run_request(
    source_job_name: str,
    source_run_id: str,
    source_partition_key: str,
) -> RunRequest:
    series = _SOURCE_JOB_TO_SERIES.get(source_job_name)
    if series is None:
        raise ValueError(f'Unknown depth snapshot source job: {source_job_name}')
    return RunRequest(
        partition_key=series,
        run_key=f'{series}:{source_run_id}',
        run_config=RunConfig(
            ops={
                'build_depth_snapshot_store_arrow': DepthSnapshotStoreConfig(
                    source_partition_key=source_partition_key
                )
            }
        ),
    )


def minute_start_from_partition_key(partition_key: str) -> datetime:
    parsed = datetime.fromisoformat(partition_key)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(second=0, microsecond=0)


def _clickhouse_datetime64(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.strftime('%Y-%m-%d %H:%M:%S.000')


def depth_snapshot_chunk_relative_path(minute_start: datetime) -> Path:
    utc_minute = minute_start.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return (
        Path('chunks') / utc_minute.strftime('%Y/%m/%d/%H') / f'{utc_minute:%Y%m%dT%H%M%SZ}.arrow'
    )


def _snapshot_rows(result: object, spec: DepthSnapshotSpec) -> tuple[DepthSnapshotRow, ...]:
    if not isinstance(result, Sequence) or isinstance(result, (bytes, str)):
        raise TypeError('Expected ClickHouse row sequence for depth snapshots.')
    if not result:
        raise RuntimeError(f'No source rows found in {spec.table_name}.')

    raw_rows = cast(Sequence[object], result)
    rows: list[DepthSnapshotRow] = []
    for index, raw_row in enumerate(raw_rows):
        row = _row_sequence(raw_row, index)
        if len(row) != 5:
            raise TypeError(f'Expected depth snapshot row {index} to have 5 values.')
        ts = _int_value(row[0], f'row {index} ts')
        rows.append(
            DepthSnapshotRow(
                ts=ts,
                source_timestamp_ms=_int_value(row[1], f'row {index} source_timestamp_ms'),
                last_update_id=_int_value(row[2], f'row {index} last_update_id'),
                bids=_book_levels(row[3], 'bids', spec.depth, ts),
                asks=_book_levels(row[4], 'asks', spec.depth, ts),
            )
        )
    return tuple(rows)


def _row_sequence(value: object, row_index: int) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (bytes, str)):
        raise TypeError(f'Expected depth snapshot row {row_index} to be a sequence.')
    return cast(Sequence[object], value)


def _int_value(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f'Expected {name} to be int, got {type(value).__name__}.')
    return value


def _float_value(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise TypeError(f'Expected {name} to be numeric, got {type(value).__name__}.')
    return float(value)


def _book_levels(
    value: object,
    side: str,
    depth: int,
    ts: int,
) -> tuple[BookLevel, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (bytes, str)):
        raise TypeError(f'Expected {side} levels at ts={ts} to be a sequence.')
    raw_levels = cast(Sequence[object], value)
    if len(raw_levels) != depth:
        raise RuntimeError(f'Expected {depth} {side} levels at ts={ts}, got {len(raw_levels)}.')

    levels: list[BookLevel] = []
    for index, raw_level in enumerate(raw_levels):
        level = _row_sequence(raw_level, index)
        if len(level) != 2:
            raise TypeError(f'Expected {side} level {index} at ts={ts} to have 2 values.')
        levels.append(
            (
                _float_value(level[0], f'{side} level {index} price at ts={ts}'),
                _float_value(level[1], f'{side} level {index} qty at ts={ts}'),
            )
        )
    return tuple(levels)


def _dedupe_sort_rows(rows: Sequence[DepthSnapshotRow]) -> tuple[DepthSnapshotRow, ...]:
    by_ts: dict[int, DepthSnapshotRow] = {}
    for row in rows:
        by_ts[row.ts] = row
    return tuple(by_ts[ts] for ts in sorted(by_ts))


def _book_array(
    rows: Sequence[DepthSnapshotRow],
    side: str,
    depth: int,
) -> pa.FixedSizeListArray:
    prices: list[float] = []
    quantities: list[float] = []
    for row in rows:
        levels = row.bids if side == 'bids' else row.asks
        for price, quantity in levels:
            prices.append(price)
            quantities.append(quantity)

    struct_values = pa.StructArray.from_arrays(
        [
            pa.array(prices, type=pa.float64()),
            pa.array(quantities, type=pa.float64()),
        ],
        names=['price', 'qty'],
    )
    return pa.FixedSizeListArray.from_arrays(struct_values, depth)


def _snapshot_table(rows: Sequence[DepthSnapshotRow], depth: int) -> pa.Table:
    return pa.table(
        {
            'ts': pa.array([row.ts for row in rows], type=pa.int64()),
            'source_timestamp_ms': pa.array(
                [row.source_timestamp_ms for row in rows], type=pa.uint64()
            ),
            'last_update_id': pa.array([row.last_update_id for row in rows], type=pa.uint64()),
            'bids': _book_array(rows, 'bids', depth),
            'asks': _book_array(rows, 'asks', depth),
        }
    )


def build_depth_snapshot_frame(
    client: ClickHouseClient,
    database: str,
    spec: DepthSnapshotSpec,
    minute_start: datetime,
) -> BarSeriesBuild:
    minute_end = minute_start + timedelta(minutes=1)
    result = client.execute(
        f"""
        SELECT
            toUnixTimestamp64Nano(datetime) AS ts,
            source_timestamp_ms,
            last_update_id,
            bids,
            asks
        FROM {database}.{spec.table_name} FINAL
        WHERE datetime >= toDateTime64('{_clickhouse_datetime64(minute_start)}', 3)
          AND datetime < toDateTime64('{_clickhouse_datetime64(minute_end)}', 3)
        ORDER BY ts ASC, source_timestamp_ms ASC
        """
    )
    rows = _snapshot_rows(result, spec)
    deduped = _dedupe_sort_rows(rows)
    table = _snapshot_table(deduped, spec.depth)
    frame = pl.from_arrow(table)
    if not isinstance(frame, pl.DataFrame):
        raise TypeError('Expected depth snapshot Arrow table to become a Polars DataFrame.')
    return BarSeriesBuild(
        frame.rechunk(),
        source_rows=len(rows),
        dropped_duplicate_ts=len(rows) - len(deduped),
    )


def _ipc_payload(df: pl.DataFrame) -> bytes:
    sink = io.BytesIO()
    df.write_ipc(sink, compression='uncompressed', record_batch_size=max(df.height, 1))
    return sink.getvalue()


def _fsync_file(handle: io.BufferedWriter) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _atomic_write_bytes(target: Path, payload: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f'.{target.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}'
    with open(tmp, 'wb') as handle:
        handle.write(payload)
        _fsync_file(handle)
    os.replace(tmp, target)


def _latest_manifest_minute(manifest_path: Path) -> datetime | None:
    if not manifest_path.exists():
        return None

    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    source_partition_key = manifest.get('source_partition_key')
    if not isinstance(source_partition_key, str):
        raise RuntimeError(f'{manifest_path} does not contain source_partition_key.')
    return minute_start_from_partition_key(source_partition_key)


def publish_depth_snapshot_chunk(
    series: str,
    source_partition_key: str,
    build: BarSeriesBuild,
) -> DepthSnapshotChunkPublish:
    payload = _ipc_payload(build.df)
    version = hashlib.sha256(payload).hexdigest()[:VERSION_HEX]
    minute_start = minute_start_from_partition_key(source_partition_key)
    relative_chunk = depth_snapshot_chunk_relative_path(minute_start)
    directory = series_store_dir(series)
    chunk = directory / relative_chunk
    _atomic_write_bytes(chunk, payload)
    manifest_path = directory / LATEST_MANIFEST_NAME

    lock_path = directory / f'.{series}.lock'
    with open(lock_path, 'w', encoding='utf-8') as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        latest_minute = _latest_manifest_minute(manifest_path)
        if latest_minute is not None and minute_start < latest_minute:
            return DepthSnapshotChunkPublish('skipped_not_newer', version, relative_chunk.as_posix())

        manifest = {
            'series': series,
            'source_partition_key': source_partition_key,
            'chunk': relative_chunk.as_posix(),
            'rows': build.df.height,
            'source_rows': build.source_rows,
            'dropped_duplicate_ts': build.dropped_duplicate_ts,
            'version': version,
            'updated_at_unix_ns': time.time_ns(),
        }
        _atomic_write_bytes(
            manifest_path,
            json.dumps(manifest, sort_keys=True).encode('utf-8') + b'\n',
        )
    return DepthSnapshotChunkPublish('published', version, relative_chunk.as_posix())


@asset(
    name='build_depth_snapshot_store_arrow',
    partitions_def=DEPTH_SNAPSHOT_PARTITIONS,
    group_name='binance_depth_snapshot_store',
    description=(
        'Projects raw Binance spot depth20/depth200 ClickHouse snapshots into '
        'minute chunk Arrow IPC files under LOCAL_ARROW_DIR. Each successful source '
        'partition writes one uncompressed record batch with fixed-size nested bid/ask '
        'book columns and atomically flips latest.json.'
    ),
)
def build_depth_snapshot_store_arrow(
    context: AssetExecutionContext,
    config: DepthSnapshotStoreConfig,
) -> dict[str, object]:
    series = context.partition_key
    source_partition_key = config.source_partition_key
    if source_partition_key is None:
        raise RuntimeError('Depth snapshot Arrow chunks require source_partition_key config.')

    minute_start = minute_start_from_partition_key(source_partition_key)
    spec = spec_for_depth_snapshot_series(series)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        build = build_depth_snapshot_frame(client, settings.database, spec, minute_start)
        if build.dropped_duplicate_ts > 0:
            context.log.warning(
                f'{series}: dropped {build.dropped_duplicate_ts} row(s) with duplicate ts to keep '
                'the index strictly increasing.'
            )

        if config.dry_run:
            return {
                'status': 'dry_run',
                'series': series,
                'source_partition_key': source_partition_key,
                'rows': build.df.height,
                'source_rows': build.source_rows,
                'dropped_duplicate_ts': build.dropped_duplicate_ts,
            }

        outcome = publish_depth_snapshot_chunk(series, source_partition_key, build)
        context.log.info(
            f'{series}: {outcome.status} chunk={outcome.chunk} version={outcome.version} '
            f'rows={build.df.height}'
        )
        return {
            'status': 'success',
            'series': series,
            'source_partition_key': source_partition_key,
            'rows': build.df.height,
            'source_rows': build.source_rows,
            'dropped_duplicate_ts': build.dropped_duplicate_ts,
            'version': outcome.version,
            'chunk': outcome.chunk,
            'outcome': outcome.status,
        }
    finally:
        client.disconnect()
