import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Protocol, cast

import polars as pl
import pyarrow as pa
from dagster import AssetExecutionContext
from huggingface_hub import HfApi

EXPORT_START_DATE = "2020-01-01 00:00:00"
DEFAULT_CLICKHOUSE_HTTP_PORT = 8123
SUPPORTED_DATETIME_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)
KLINE_EXPORT_COLUMNS = [
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "mean",
    "std",
    "volume",
    "maker_ratio",
    "no_of_trades",
    "open_liquidity",
    "high_liquidity",
    "low_liquidity",
    "close_liquidity",
    "liquidity_sum",
    "maker_volume",
    "maker_liquidity",
]


class _ClickHouseArrowClientProtocol(Protocol):
    def query_arrow(
        self,
        query: str,
        parameters: Mapping[str, object] | None = None,
    ) -> pa.Table:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def _get_clickhouse_http_port() -> int:
    value = os.environ.get("CLICKHOUSE_HTTP_PORT", str(DEFAULT_CLICKHOUSE_HTTP_PORT))
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError("CLICKHOUSE_HTTP_PORT environment variable must be an integer.") from exc


def _make_clickhouse_arrow_client() -> _ClickHouseArrowClientProtocol:
    client_factory = getattr(import_module("clickhouse_connect"), "get_client")
    return cast(
        _ClickHouseArrowClientProtocol,
        client_factory(
            host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port=_get_clickhouse_http_port(),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ["CLICKHOUSE_PASSWORD"],
        ),
    )


def _get_huggingface_token() -> str:
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGINGFACE_HUB_TOKEN")
    if not token:
        raise RuntimeError(
            "HF_TOKEN or HUGGINGFACE_HUB_TOKEN must be set before publishing to Hugging Face."
        )

    return token


def _get_huggingface_dataset_repo_id(
    *,
    repo_id_env: str | None,
    default_repo_id: str,
) -> str:
    if repo_id_env is None:
        return default_repo_id

    return os.environ.get(repo_id_env, default_repo_id)


def _validate_clickhouse_identifier(value: str, field_name: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_]+", value) is None:
        raise ValueError(f"Invalid ClickHouse {field_name}: {value}")
    return value


def _normalize_datetime_literal(value: str, field_name: str) -> str:
    last_error: ValueError | None = None
    for fmt in SUPPORTED_DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            last_error = exc

    message = (
        f"{field_name} must match one of: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, YYYY-MM-DDTHH:MM:SS."
    )
    if last_error is None:
        raise ValueError(message)
    raise ValueError(message) from last_error


def _validate_projection_kline_size(kline_size_seconds: int) -> int:
    if type(kline_size_seconds) is not int:
        raise TypeError("kline_size_seconds must be an int.")
    if kline_size_seconds < 60:
        raise ValueError("kline_size_seconds must be at least 60.")
    if kline_size_seconds % 60 != 0:
        raise ValueError("kline_size_seconds must be a multiple of the 60-second projection.")
    return kline_size_seconds


def _get_binance_spot_klines_from_1m_projection(
    *,
    kline_size_seconds: int,
    start_date_limit: str,
    end_date_limit: str,
    table_name: str,
    database_name: str,
) -> pl.DataFrame:
    kline_size_seconds = _validate_projection_kline_size(kline_size_seconds)
    table_name = _validate_clickhouse_identifier(table_name, "table name")
    database_name = _validate_clickhouse_identifier(database_name, "database name")
    start_date_limit = _normalize_datetime_literal(start_date_limit, "start_date_limit")
    end_date_limit = _normalize_datetime_literal(end_date_limit, "end_date_limit")
    client = _make_clickhouse_arrow_client()

    try:
        arrow_table = client.query_arrow(
            f"""
            SELECT
                kline_datetime AS datetime,
                argMin(source_open, source_datetime) AS open,
                max(source_high) AS high,
                min(source_low) AS low,
                argMax(source_close, source_datetime) AS close,
                sum(source_no_of_trades * source_mean) / sum(source_no_of_trades) AS mean,
                sqrt(
                    greatest(
                        sum(source_no_of_trades * ((source_std * source_std) + (source_mean * source_mean))) / sum(source_no_of_trades)
                        - pow(sum(source_no_of_trades * source_mean) / sum(source_no_of_trades), 2),
                        0
                    )
                ) AS std,
                sumKahan(source_volume) AS volume,
                sum(source_no_of_trades * source_maker_ratio) / sum(source_no_of_trades) AS maker_ratio,
                sum(source_no_of_trades) AS no_of_trades,
                argMin(source_open_liquidity, source_datetime) AS open_liquidity,
                max(source_high_liquidity) AS high_liquidity,
                min(source_low_liquidity) AS low_liquidity,
                argMax(source_close_liquidity, source_datetime) AS close_liquidity,
                sum(source_liquidity_sum) AS liquidity_sum,
                sumKahan(source_maker_volume) AS maker_volume,
                sum(source_maker_liquidity) AS maker_liquidity
            FROM (
                SELECT
                    datetime AS source_datetime,
                    open AS source_open,
                    high AS source_high,
                    low AS source_low,
                    close AS source_close,
                    mean AS source_mean,
                    std AS source_std,
                    volume AS source_volume,
                    maker_ratio AS source_maker_ratio,
                    no_of_trades AS source_no_of_trades,
                    open_liquidity AS source_open_liquidity,
                    high_liquidity AS source_high_liquidity,
                    low_liquidity AS source_low_liquidity,
                    close_liquidity AS source_close_liquidity,
                    liquidity_sum AS source_liquidity_sum,
                    maker_volume AS source_maker_volume,
                    maker_liquidity AS source_maker_liquidity,
                    toDateTime({{bucket_seconds:UInt32}} * intDiv(toUnixTimestamp(datetime), {{bucket_seconds:UInt32}})) AS kline_datetime
                FROM {database_name}.{table_name}
                WHERE datetime >= toDateTime({{start_dt:String}})
                  AND datetime < toDateTime({{end_dt:String}})
            )
            GROUP BY kline_datetime
            ORDER BY kline_datetime ASC
            """,
            parameters={
                "bucket_seconds": kline_size_seconds,
                "start_dt": start_date_limit,
                "end_dt": end_date_limit,
            },
        )
    finally:
        client.close()

    data = cast(pl.DataFrame, pl.from_arrow(arrow_table)).select(KLINE_EXPORT_COLUMNS)
    if data.height == 0:
        return data

    return data.with_columns([
        pl.col("datetime").cast(pl.Datetime("ms", time_zone="UTC")),
        pl.col("mean").round(5),
        pl.col("std").round(6),
        pl.col("volume").round(9),
        pl.col("liquidity_sum").round(1),
        pl.col("maker_liquidity").round(1),
    ]).sort("datetime")


def _build_dataset_card(
    *,
    export_end_date: str,
    row_count: int,
    file_name: str,
    cadence_label: str,
    resolution_label: str,
) -> str:
    return f"""# BTCUSDT {cadence_label} spot klines

This dataset is exported daily from `origo.binance_spot_klines` at {resolution_label} resolution.

Latest snapshot:

- file: `{file_name}`
- start date: `2020-01-01`
- rows: `{row_count}`
- end date: `{export_end_date}`
- columns: `datetime`, `open`, `high`, `low`, `close`, `mean`, `std`, `volume`, `maker_ratio`, `no_of_trades`, `open_liquidity`, `high_liquidity`, `low_liquidity`, `close_liquidity`, `liquidity_sum`, `maker_volume`, `maker_liquidity`

Notes:

- Source market: Binance spot BTCUSDT
- Source table: `origo.binance_spot_klines`
- `median` and `iqr` are intentionally omitted from the exported Parquet snapshot
- Timestamps are UTC
"""


def _build_snapshot_metadata(
    export_end_date: str,
    file_name: str,
    row_count: int,
    file_sha256: str,
) -> str:
    generated_at = datetime.now(UTC).isoformat()
    return (
        json.dumps(
            {
                "file_name": file_name,
                "export_start_date": "2020-01-01",
                "export_end_date": export_end_date,
                "row_count": row_count,
                "sha256": file_sha256,
                "generated_at_utc": generated_at,
            },
            indent=2,
        )
        + "\n"
    )


def _sha256_for_file(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def publish_binance_spot_kline_snapshot_to_huggingface(
    context: AssetExecutionContext,
    *,
    kline_size_seconds: int,
    file_prefix: str,
    default_repo_id: str,
    repo_id_env: str | None,
    cadence_label: str,
    resolution_label: str,
) -> dict[str, object]:
    partition_date_str = context.asset_partition_key_for_output()
    export_end_date = partition_date_str
    export_end_exclusive = (
        datetime.strptime(export_end_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d 00:00:00")
    dataset_repo_id = _get_huggingface_dataset_repo_id(
        repo_id_env=repo_id_env,
        default_repo_id=default_repo_id,
    )

    file_name = f"{file_prefix}{export_end_date.replace('-', '')}.parquet"

    context.log.info(
        f"Building Binance spot {cadence_label} klines snapshot through {export_end_date} UTC."
    )
    data = _get_binance_spot_klines_from_1m_projection(
        kline_size_seconds=kline_size_seconds,
        start_date_limit=EXPORT_START_DATE,
        end_date_limit=export_end_exclusive,
        table_name="binance_spot_klines",
        database_name="origo",
    )

    if data.height == 0:
        raise RuntimeError(
            f"No {cadence_label} kline rows were returned for export through {export_end_date}."
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        parquet_path = tmp_path / file_name
        readme_path = tmp_path / "README.md"
        metadata_path = tmp_path / "latest.json"

        context.log.info(f"Writing snapshot to {parquet_path}.")
        data.write_parquet(parquet_path, compression="zstd")
        file_sha256 = _sha256_for_file(parquet_path)

        readme_path.write_text(
            _build_dataset_card(
                export_end_date=export_end_date,
                row_count=data.height,
                file_name=file_name,
                cadence_label=cadence_label,
                resolution_label=resolution_label,
            ),
            encoding="utf-8",
        )
        metadata_path.write_text(
            _build_snapshot_metadata(
                export_end_date=export_end_date,
                file_name=file_name,
                row_count=data.height,
                file_sha256=file_sha256,
            ),
            encoding="utf-8",
        )

        api = HfApi(token=_get_huggingface_token())
        api.create_repo(
            repo_id=dataset_repo_id,
            repo_type="dataset",
            exist_ok=True,
        )

        commit_message = f"Add BTCUSDT {cadence_label} klines snapshot through {export_end_date}"
        api.upload_folder(
            folder_path=str(tmp_path),
            repo_id=dataset_repo_id,
            repo_type="dataset",
            commit_message=commit_message,
            delete_patterns=[f"{file_prefix}*.parquet"],
        )

    latest_datetime = data["datetime"].max()
    context.log.info(
        f"Published {file_name} to {dataset_repo_id} with {data.height} rows."
    )

    return {
        "repo_id": dataset_repo_id,
        "file_name": file_name,
        "rows_exported": data.height,
        "latest_datetime": str(latest_datetime),
        "sha256": file_sha256,
    }
