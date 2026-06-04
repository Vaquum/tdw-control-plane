"""Mirror the 12 HF Binance spot kline series to monthly Parquet files on a
local mount, refreshed every 10 minutes.

Stateless: each tick rebuilds the open month(s) from the Origo projections and
replaces the month file atomically. The filesystem is the only state. A
monotonic guard ensures a slow, out-of-order tick can never replace a fresher
file with staler data (atomic != monotonic). A separate ``backfill`` mode
rebuilds every month from 2020-01 for the initial fill or after a gap.
"""

import os
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import polars as pl
from dagster import AssetExecutionContext, AssetsDefinition, Config, asset

from tdw_control_plane.query.binance_spot_kline_rollups import dollar_month, time_month

DEFAULT_MOUNT_DIR = "/opt/parquet"
EXPORT_START_YEAR = 2020
EXPORT_START_MONTH = 1
ORPHAN_TEMP_MAX_AGE_SECONDS = 3600


@dataclass(frozen=True)
class MountKlineSpec:
    name: str
    family: Literal["time", "dollar"]
    size: int  # interval minutes (time) | dollar ratio (dollar)
    sub_path: str  # e.g. "time/1m" or "dollar/1M"


SPECS: tuple[MountKlineSpec, ...] = (
    MountKlineSpec("time_1m", "time", 1, "time/1m"),
    MountKlineSpec("time_15m", "time", 15, "time/15m"),
    MountKlineSpec("time_30m", "time", 30, "time/30m"),
    MountKlineSpec("time_1h", "time", 60, "time/1h"),
    MountKlineSpec("time_2h", "time", 120, "time/2h"),
    MountKlineSpec("time_4h", "time", 240, "time/4h"),
    MountKlineSpec("dollar_1M", "dollar", 1, "dollar/1M"),
    MountKlineSpec("dollar_15M", "dollar", 15, "dollar/15M"),
    MountKlineSpec("dollar_30M", "dollar", 30, "dollar/30M"),
    MountKlineSpec("dollar_60M", "dollar", 60, "dollar/60M"),
    MountKlineSpec("dollar_120M", "dollar", 120, "dollar/120M"),
    MountKlineSpec("dollar_240M", "dollar", 240, "dollar/240M"),
)


class MountExportConfig(Config):
    mode: str = "tick"  # "tick" -> open month(s); "backfill" -> every month from 2020-01
    dry_run: bool = False


def _mount_dir() -> Path:
    return Path(os.environ.get("LOCAL_PARQUET_DIR", DEFAULT_MOUNT_DIR))


def month_path(sub_path: str, year: int, month: int) -> Path:
    return _mount_dir() / sub_path / f"{year:04d}" / f"{month:02d}.parquet"


def _timestamp_column(family: str) -> str:
    return "datetime" if family == "time" else "end_datetime"


def _previous_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def _month_range(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def months_for_run(mode: str, now: datetime) -> list[tuple[int, int]]:
    """Tick rebuilds the previous + current month (the only ones that can still
    change); the monotonic guard turns the finalized previous month into a no-op
    once it stops growing. Backfill rebuilds every month from the export start."""
    if mode == "backfill":
        return _month_range(EXPORT_START_YEAR, EXPORT_START_MONTH, now.year, now.month)
    if mode == "tick":
        prev_year, prev_month = _previous_month(now.year, now.month)
        return _month_range(prev_year, prev_month, now.year, now.month)
    raise ValueError(f"Unknown mount export mode: {mode}")


def _build_month_df(spec: MountKlineSpec, year: int, month: int) -> pl.DataFrame:
    if spec.family == "time":
        return time_month(interval_minutes=spec.size, year=year, month=month)
    return dollar_month(ratio=spec.size, year=year, month=month)


def _existing_max_timestamp(target: Path, column: str) -> datetime | None:
    value = (
        pl.scan_parquet(target)
        .select(pl.col(column).max().alias("m"))
        .collect()
        .item()
    )
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise RuntimeError(f"Unexpected {column} type in {target}: {type(value)!r}")
    return value


def _clear_orphan_temp_files(directory: Path, month: int) -> None:
    prefix = f".{month:02d}.parquet.partial-"
    cutoff = time.time() - ORPHAN_TEMP_MAX_AGE_SECONDS
    for orphan in directory.glob(f"{prefix}*"):
        if orphan.stat().st_mtime < cutoff:
            orphan.unlink()


def write_month_atomic(df: pl.DataFrame, spec: MountKlineSpec, year: int, month: int) -> str:
    """Write a month file atomically (temp in same dir -> os.replace), but skip
    the replace when the existing file already holds data at least as fresh.
    Returns ``written``, ``skipped_empty``, or ``skipped_not_newer``."""
    if df.height == 0:
        return "skipped_empty"

    column = _timestamp_column(spec.family)
    new_max = df[column].max()
    if not isinstance(new_max, datetime):
        raise RuntimeError(f"{spec.name} {column} max is not a datetime: {new_max!r}")

    target = month_path(spec.sub_path, year, month)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        existing_max = _existing_max_timestamp(target, column)
        if existing_max is not None and existing_max >= new_max:
            return "skipped_not_newer"

    _clear_orphan_temp_files(target.parent, month)
    tmp = target.parent / f".{month:02d}.parquet.partial-{os.getpid()}-{uuid.uuid4().hex}"
    df.write_parquet(tmp, compression="zstd")
    os.replace(tmp, target)
    return "written"


def run_mount_export(
    context: AssetExecutionContext, spec: MountKlineSpec, config: MountExportConfig
) -> dict[str, object]:
    now = datetime.now(UTC)
    months = months_for_run(config.mode, now)
    results: dict[str, str] = {}
    for year, month in months:
        key = f"{year:04d}-{month:02d}"
        if config.dry_run:
            results[key] = "dry_run"
            continue
        df = _build_month_df(spec, year, month)
        outcome = write_month_atomic(df, spec, year, month)
        results[key] = outcome
        context.log.info(f"{spec.name} {key}: {outcome} ({df.height} rows)")

    return {
        "status": "success",
        "series": spec.name,
        "mode": config.mode,
        "months": results,
    }


def build_mount_export_asset(spec: MountKlineSpec) -> AssetsDefinition:
    @asset(
        name=f"export_{spec.name}_to_mount",
        group_name="binance_local_parquet",
        description=(
            f"Mirrors BTCUSDT {spec.sub_path} klines to monthly Parquet on the local "
            f"mount (LOCAL_PARQUET_DIR, default {DEFAULT_MOUNT_DIR}). Stateless: rebuilds "
            f"the open month(s) each run with a monotonic atomic replace. To force-rebuild "
            f"a finalized month, delete its file and run the backfill job (mode='backfill')."
        ),
    )
    def _asset(context: AssetExecutionContext, config: MountExportConfig) -> dict[str, object]:
        return run_mount_export(context, spec, config)

    return _asset


MOUNT_EXPORT_ASSETS: list[AssetsDefinition] = [build_mount_export_asset(spec) for spec in SPECS]
