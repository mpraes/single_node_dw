"""Shared helpers for stream message normalization and Parquet micro-batch writes."""

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ..data_contract import IngestedItem, IngestionResult


def decode_payload(payload: bytes | str | None) -> dict | list | str | None:
    """Decode bytes payload and parse JSON when possible."""
    if payload is None:
        return None

    if isinstance(payload, bytes):
        decoded = payload.decode("utf-8", errors="replace")
    else:
        decoded = payload

    try:
        return json.loads(decoded)
    except Exception:
        return decoded


def build_record(
    protocol: str,
    stream_name: str,
    payload: bytes | str | None,
    message_key: bytes | str | None = None,
    metadata: dict | None = None,
) -> dict[str, str | int | float | bool | None]:
    """Normalize one stream message into a serializable event record."""
    metadata_value = metadata or {}
    payload_value = decode_payload(payload)

    if isinstance(message_key, bytes):
        key_value = message_key.decode("utf-8", errors="replace")
    else:
        key_value = message_key

    return {
        "protocol": protocol,
        "stream": stream_name,
        "event_time": datetime.now(UTC).isoformat(),
        "message_key": key_value,
        "payload": json.dumps(payload_value, ensure_ascii=False) if payload_value is not None else None,
        "metadata": json.dumps(metadata_value, ensure_ascii=False),
    }


def write_micro_batch_to_parquet(
    records: list[dict[str, str | int | float | bool | None]],
    lake_path: str,
    protocol: str,
    stream_name: str,
) -> tuple[str, int]:
    """Persist records as a Parquet micro-batch and return path and file size."""
    lake_root = Path(lake_path)
    protocol_root = lake_root / protocol
    protocol_root.mkdir(parents=True, exist_ok=True)

    safe_stream = stream_name.replace("/", "_").replace(".", "_")
    file_name = f"{safe_stream}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}.parquet"
    target_path = protocol_root / file_name

    frame = pl.DataFrame(records)
    frame.write_parquet(target_path)

    return str(target_path), target_path.stat().st_size


def build_success_result(
    protocol: str,
    stream_name: str,
    records: list[dict[str, str | int | float | bool | None]],
    lake_path: str,
) -> IngestionResult:
    """Build standardized ingestion result for stream micro-batch execution."""
    if not records:
        return IngestionResult(
            protocol=protocol,
            success=True,
            items=[],
            metadata={"stream": stream_name, "messages": 0},
        )

    parquet_path, parquet_size = write_micro_batch_to_parquet(
        records=records,
        lake_path=lake_path,
        protocol=protocol,
        stream_name=stream_name,
    )
    return IngestionResult(
        protocol=protocol,
        success=True,
        items=[
            IngestedItem(
                source_path=stream_name,
                lake_path=parquet_path,
                size_bytes=parquet_size,
                payload={"messages": len(records)},
            )
        ],
        metadata={"stream": stream_name, "messages": len(records), "format": "parquet"},
    )
