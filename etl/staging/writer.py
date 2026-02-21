from datetime import datetime, timezone as UTC
from pathlib import Path

import polars as pl

from connections._logging import get_logger
from connections.sources.data_contract import IngestionResult

logger = get_logger("staging.writer")


def _payload_to_rows(payload: dict | list | str | int | float | bool | None) -> list[dict]:
    if payload is None:
        return []

    if isinstance(payload, dict):
        return [payload]

    if isinstance(payload, list):
        rows: list[dict] = []
        for item in payload:
            if isinstance(item, dict):
                rows.append(item)
            else:
                rows.append({"payload": item})
        return rows

    return [{"payload": payload}]


def _safe_name(value: str) -> str:
    return value.replace("/", "_").replace(".", "_")


def write_ingestion_result_to_parquet(
    result: IngestionResult,
    lake_path: str,
    source_name: str,
) -> list[str]:
    if not result.success or not result.items:
        return []

    paths: list[str] = []
    safe_source_name = _safe_name(source_name)

    for item in result.items:
        if item.lake_path:
            paths.append(item.lake_path)
            continue

        rows = _payload_to_rows(item.payload)
        if not rows:
            continue

        now = datetime.now(UTC.utc)
        partition = now.strftime("%Y-%m-%d")
        file_timestamp = now.strftime("%Y%m%dT%H%M%S%fZ")
        ingested_at = now.isoformat()

        target_dir = Path(lake_path) / result.protocol / source_name / partition
        target_dir.mkdir(parents=True, exist_ok=True)

        target_path = target_dir / f"{safe_source_name}_{file_timestamp}.parquet"

        frame = pl.DataFrame(rows).with_columns(pl.lit(ingested_at).alias("_ingested_at"))
        frame.write_parquet(target_path)

        target = str(target_path)
        paths.append(target)
        logger.info(
            "Parquet written for ingestion result",
            extra={
                "protocol": result.protocol,
                "source": source_name,
                "rows": frame.height,
                "path": target,
            },
        )

    return paths
