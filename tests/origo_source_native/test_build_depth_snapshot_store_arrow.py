from __future__ import annotations

import os
import json
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest
from dagster import materialize

os.environ.setdefault('CLICKHOUSE_PASSWORD', 'import-guard')

import tdw_control_plane.assets.build_depth_snapshot_store_arrow as depth_store
from tdw_control_plane.assets.build_bar_store_arrow import series_store_dir
from tdw_control_plane.assets.build_depth_snapshot_store_arrow import (
    DEPTH20_SOURCE_JOB_NAME,
    DEPTH200_SOURCE_JOB_NAME,
    DEPTH_SNAPSHOT_PARTITIONS,
    DEPTH_SNAPSHOT_SERIES,
    DepthSnapshotSpec,
    LATEST_MANIFEST_NAME,
    build_depth_snapshot_frame,
    build_depth_snapshot_store_arrow,
    depth_snapshot_chunk_relative_path,
    depth_snapshot_store_partition_run_request,
    minute_start_from_partition_key,
    spec_for_depth_snapshot_series,
)

SOURCE_PARTITION_KEY = '2026-06-14T12:57:00+0000'

SnapshotRow = tuple[int, int, int, list[tuple[float, float]], list[tuple[float, float]]]


class FakeClickHouseClient:
    def __init__(self, rows: Sequence[SnapshotRow]) -> None:
        self.rows = rows
        self.queries: list[str] = []
        self.disconnected = False

    def execute(
        self,
        query: str,
        params: object | None = None,
        settings: object | None = None,
    ) -> object:
        self.queries.append(query)
        return self.rows

    def disconnect(self) -> None:
        self.disconnected = True


def _levels(depth: int, start: float) -> list[tuple[float, float]]:
    return [(start + index, (index + 1) / 10) for index in range(depth)]


def _row(ts: int, source_timestamp_ms: int, last_update_id: int, depth: int) -> SnapshotRow:
    return (
        ts,
        source_timestamp_ms,
        last_update_id,
        _levels(depth, 100.0),
        _levels(depth, 200.0),
    )


def _field_array(table: pa.Table, column: str, field: str) -> pa.Array:
    book = table.column(column).chunk(0)
    if not isinstance(book, pa.FixedSizeListArray):
        raise TypeError(f'{column} must be a FixedSizeListArray')
    values = book.values
    if not isinstance(values, pa.StructArray):
        raise TypeError(f'{column} values must be a StructArray')
    return values.field(field)


def test_depth_snapshot_series_cover_depth20_and_depth200() -> None:
    assert DEPTH_SNAPSHOT_SERIES == ('depth20_snapshots', 'depth200_snapshots')
    assert set(DEPTH_SNAPSHOT_PARTITIONS.get_partition_keys()) == set(DEPTH_SNAPSHOT_SERIES)

    depth20 = spec_for_depth_snapshot_series('depth20_snapshots')
    depth200 = spec_for_depth_snapshot_series('depth200_snapshots')

    assert depth20.table_name == 'binance_spot_depth20_snapshots'
    assert depth20.depth == 20
    assert depth200.table_name == 'binance_spot_depth200_snapshots'
    assert depth200.depth == 200


def test_build_depth_snapshot_frame_shapes_fixed_size_books_and_zero_copy_buffers() -> None:
    spec = DepthSnapshotSpec('test_depth_snapshots', 'test_table', 2)
    rows = [
        _row(2, 20, 200, spec.depth),
        _row(1, 10, 100, spec.depth),
        _row(2, 22, 202, spec.depth),
    ]

    client = FakeClickHouseClient(rows)
    build = build_depth_snapshot_frame(
        client,
        'origo',
        spec,
        minute_start_from_partition_key(SOURCE_PARTITION_KEY),
    )
    frame = build.df
    table = frame.to_arrow()

    assert 'WHERE datetime >=' in client.queries[0]
    assert '2026-06-14 12:57:00.000' in client.queries[0]
    assert '2026-06-14 12:58:00.000' in client.queries[0]
    assert frame.columns == ['ts', 'source_timestamp_ms', 'last_update_id', 'bids', 'asks']
    assert frame['ts'].to_list() == [1, 2]
    assert frame['source_timestamp_ms'].to_list() == [10, 22]
    assert build.source_rows == 3
    assert build.dropped_duplicate_ts == 1
    assert frame.n_chunks() == 1

    bids = table.column('bids').chunk(0)
    asks = table.column('asks').chunk(0)
    assert isinstance(bids, pa.FixedSizeListArray)
    assert isinstance(asks, pa.FixedSizeListArray)
    assert bids.type.list_size == spec.depth
    assert asks.type.list_size == spec.depth

    ts_view = table.column('ts').chunk(0).to_numpy(zero_copy_only=True)
    bid_prices = _field_array(table, 'bids', 'price').to_numpy(zero_copy_only=True)
    bid_quantities = _field_array(table, 'bids', 'qty').to_numpy(zero_copy_only=True)

    assert ts_view.tolist() == [1, 2]
    assert bid_prices.reshape(frame.height, spec.depth).tolist() == [[100.0, 101.0], [100.0, 101.0]]
    assert bid_quantities.reshape(frame.height, spec.depth).tolist() == [[0.1, 0.2], [0.1, 0.2]]


def test_build_depth_snapshot_frame_rejects_empty_source() -> None:
    spec = DepthSnapshotSpec('test_depth_snapshots', 'test_table', 2)

    with pytest.raises(RuntimeError, match='No source rows found in test_table'):
        build_depth_snapshot_frame(
            FakeClickHouseClient([]),
            'origo',
            spec,
            minute_start_from_partition_key(SOURCE_PARTITION_KEY),
        )


def test_build_depth_snapshot_frame_rejects_wrong_book_depth() -> None:
    spec = DepthSnapshotSpec('test_depth_snapshots', 'test_table', 2)
    bad_rows = [(1, 10, 100, _levels(1, 100.0), _levels(2, 200.0))]

    with pytest.raises(RuntimeError, match='Expected 2 bids levels at ts=1, got 1'):
        build_depth_snapshot_frame(
            FakeClickHouseClient(bad_rows),
            'origo',
            spec,
            minute_start_from_partition_key(SOURCE_PARTITION_KEY),
        )


def test_asset_publishes_depth_snapshot_arrow_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = FakeClickHouseClient([_row(1, 10, 100, 20), _row(2, 20, 200, 20)])

    def client_factory(settings: object) -> FakeClickHouseClient:
        return fake_client

    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'test-password')
    monkeypatch.setattr(depth_store, 'make_clickhouse_client', client_factory)

    result = materialize(
        [build_depth_snapshot_store_arrow],
        partition_key='depth20_snapshots',
        run_config={
            'ops': {
                'build_depth_snapshot_store_arrow': {
                    'config': {'source_partition_key': SOURCE_PARTITION_KEY}
                }
            }
        },
    )

    assert result.success
    assert fake_client.disconnected
    assert 'binance_spot_depth20_snapshots' in fake_client.queries[0]

    manifest_path = series_store_dir('depth20_snapshots') / LATEST_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    chunk = series_store_dir('depth20_snapshots') / manifest['chunk']

    assert manifest['series'] == 'depth20_snapshots'
    assert manifest['source_partition_key'] == SOURCE_PARTITION_KEY
    assert chunk == series_store_dir('depth20_snapshots') / depth_snapshot_chunk_relative_path(
        minute_start_from_partition_key(SOURCE_PARTITION_KEY)
    )

    table = pa_ipc.open_file(pa.memory_map(str(chunk), 'r')).read_all()
    assert table.num_rows == 2
    assert table.column_names == ['ts', 'source_timestamp_ms', 'last_update_id', 'bids', 'asks']
    assert table.column('ts').chunk(0).to_numpy(zero_copy_only=True).tolist() == [1, 2]
    assert isinstance(table.column('bids').chunk(0), pa.FixedSizeListArray)


def test_asset_publishes_single_record_batch_ipc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_client = FakeClickHouseClient([_row(index, index, index, 20) for index in range(200_000)])

    def client_factory(settings: object) -> FakeClickHouseClient:
        return fake_client

    monkeypatch.setenv('LOCAL_ARROW_DIR', str(tmp_path))
    monkeypatch.setenv('CLICKHOUSE_PASSWORD', 'test-password')
    monkeypatch.setattr(depth_store, 'make_clickhouse_client', client_factory)

    result = materialize(
        [build_depth_snapshot_store_arrow],
        partition_key='depth20_snapshots',
        run_config={
            'ops': {
                'build_depth_snapshot_store_arrow': {
                    'config': {'source_partition_key': SOURCE_PARTITION_KEY}
                }
            }
        },
    )

    assert result.success
    manifest_path = series_store_dir('depth20_snapshots') / LATEST_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    reader = pa_ipc.open_file(
        pa.memory_map(str(series_store_dir('depth20_snapshots') / manifest['chunk']), 'r')
    )
    assert reader.num_record_batches == 1
    assert reader.read_all().column('ts').num_chunks == 1


def test_depth_snapshot_partition_run_request_maps_source_jobs() -> None:
    depth20 = depth_snapshot_store_partition_run_request(
        DEPTH20_SOURCE_JOB_NAME,
        'run-20',
        SOURCE_PARTITION_KEY,
    )
    depth200 = depth_snapshot_store_partition_run_request(
        DEPTH200_SOURCE_JOB_NAME,
        'run-200',
        SOURCE_PARTITION_KEY,
    )

    assert depth20.partition_key == 'depth20_snapshots'
    assert depth20.run_key == 'depth20_snapshots:run-20'
    assert (
        depth20.run_config['ops']['build_depth_snapshot_store_arrow']['config'][
            'source_partition_key'
        ]
        == SOURCE_PARTITION_KEY
    )
    assert depth200.partition_key == 'depth200_snapshots'
    assert depth200.run_key == 'depth200_snapshots:run-200'
    assert (
        depth200.run_config['ops']['build_depth_snapshot_store_arrow']['config'][
            'source_partition_key'
        ]
        == SOURCE_PARTITION_KEY
    )

    with pytest.raises(ValueError, match='Unknown depth snapshot source job: other_job'):
        depth_snapshot_store_partition_run_request('other_job', 'run-other', SOURCE_PARTITION_KEY)


def test_definitions_wires_depth_snapshot_arrow_job(origo_definitions_module: object) -> None:
    job = getattr(origo_definitions_module, 'build_depth_snapshot_store_arrow_job')
    asset_def = getattr(origo_definitions_module, 'build_depth_snapshot_store_arrow')

    assert job.name == 'build_depth_snapshot_store_arrow_job'
    assert asset_def.partitions_def.get_partition_keys() == list(DEPTH_SNAPSHOT_SERIES)


def test_definitions_wires_depth_snapshot_arrow_sensor_to_depth_source_jobs(
    origo_definitions_module: object,
) -> None:
    sensor = getattr(origo_definitions_module, 'depth_snapshot_store_source_sensor')
    arrow_job = getattr(origo_definitions_module, 'build_depth_snapshot_store_arrow_job')
    depth20_job = getattr(origo_definitions_module, 'refresh_binance_spot_depth20_data_source_job')
    depth200_job = getattr(
        origo_definitions_module, 'refresh_binance_spot_depth200_data_source_job'
    )

    assert sensor.name == 'depth_snapshot_store_source_sensor'
    assert arrow_job.name == 'build_depth_snapshot_store_arrow_job'
    assert depth20_job.name == DEPTH20_SOURCE_JOB_NAME
    assert depth200_job.name == DEPTH200_SOURCE_JOB_NAME
