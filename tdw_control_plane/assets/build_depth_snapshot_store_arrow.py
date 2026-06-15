from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

import polars as pl
import pyarrow as pa
from dagster import AssetExecutionContext, Config, StaticPartitionsDefinition, asset

from tdw_control_plane.assets.build_bar_store_arrow import BarSeriesBuild, publish_series
from tdw_control_plane.assets.create_binance_spot_depth20_snapshots_table_origo import (
    ClickHouseClient,
    get_clickhouse_settings,
    make_clickhouse_client,
)
from tdw_control_plane.assets.create_binance_spot_depth20_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH20_SNAPSHOTS_TABLE_NAME,
)
from tdw_control_plane.assets.create_binance_spot_depth200_snapshots_table_origo import (
    SNAPSHOTS_TABLE_NAME as DEPTH200_SNAPSHOTS_TABLE_NAME,
)

DEPTH_SNAPSHOT_SERIES: tuple[str, ...] = ('depth20_snapshots', 'depth200_snapshots')
DEPTH_SNAPSHOT_PARTITIONS = StaticPartitionsDefinition(list(DEPTH_SNAPSHOT_SERIES))

BookLevel = tuple[float, float]


class DepthSnapshotStoreConfig(Config):
    dry_run: bool = False


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


_DEPTH_SNAPSHOT_SPECS: tuple[DepthSnapshotSpec, ...] = (
    DepthSnapshotSpec('depth20_snapshots', DEPTH20_SNAPSHOTS_TABLE_NAME, 20),
    DepthSnapshotSpec('depth200_snapshots', DEPTH200_SNAPSHOTS_TABLE_NAME, 200),
)


def spec_for_depth_snapshot_series(series: str) -> DepthSnapshotSpec:
    for spec in _DEPTH_SNAPSHOT_SPECS:
        if spec.series == series:
            return spec
    raise ValueError(f'Unknown depth snapshot series: {series}')


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
) -> BarSeriesBuild:
    result = client.execute(
        f"""
        SELECT
            toUnixTimestamp64Nano(datetime) AS ts,
            source_timestamp_ms,
            last_update_id,
            bids,
            asks
        FROM {database}.{spec.table_name} FINAL
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


@asset(
    name='build_depth_snapshot_store_arrow',
    partitions_def=DEPTH_SNAPSHOT_PARTITIONS,
    group_name='binance_depth_snapshot_store',
    description=(
        'Projects raw Binance spot depth20/depth200 ClickHouse snapshots into '
        'versioned, mmap-ready Arrow IPC files under LOCAL_ARROW_DIR. Each partition '
        'writes one uncompressed record batch with fixed-size nested bid/ask book columns.'
    ),
)
def build_depth_snapshot_store_arrow(
    context: AssetExecutionContext,
    config: DepthSnapshotStoreConfig,
) -> dict[str, object]:
    series = context.partition_key
    spec = spec_for_depth_snapshot_series(series)
    settings = get_clickhouse_settings()
    client = make_clickhouse_client(settings)

    try:
        build = build_depth_snapshot_frame(client, settings.database, spec)
        if build.dropped_duplicate_ts > 0:
            context.log.warning(
                f'{series}: dropped {build.dropped_duplicate_ts} row(s) with duplicate ts to keep '
                'the index strictly increasing.'
            )

        if config.dry_run:
            return {
                'status': 'dry_run',
                'series': series,
                'rows': build.df.height,
                'source_rows': build.source_rows,
                'dropped_duplicate_ts': build.dropped_duplicate_ts,
            }

        outcome = publish_series(series, build)
        context.log.info(
            f'{series}: {outcome.status} version={outcome.version} rows={build.df.height} '
            f'reaped={len(outcome.reaped)}'
        )
        return {
            'status': 'success',
            'series': series,
            'rows': build.df.height,
            'source_rows': build.source_rows,
            'dropped_duplicate_ts': build.dropped_duplicate_ts,
            'version': outcome.version,
            'outcome': outcome.status,
            'reaped': len(outcome.reaped),
        }
    finally:
        client.disconnect()
