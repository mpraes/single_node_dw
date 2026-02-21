from pathlib import Path

import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine

from connections._logging import get_logger
from staging.dw_schema import _quote_identifier, ensure_table_exists

logger = get_logger("staging.loader")


def _write_frame_to_db(
    engine: Engine,
    frame: pl.DataFrame,
    table_name: str,
    schema: str | None = "public",
) -> int:
    """Write Polars DataFrame to DB using SQLAlchemy insert."""
    if frame.is_empty():
        return 0

    if schema:
        qualified_table = f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"
    else:
        qualified_table = _quote_identifier(table_name)

    # Convert polars frame to list of dicts for SQLAlchemy
    records = frame.to_dicts()

    # Build INSERT statement
    columns = [_quote_identifier(c) for c in frame.columns]
    placeholders = [f":{c}" for c in frame.columns]

    sql = f"""
    INSERT INTO {qualified_table} ({", ".join(columns)})
    VALUES ({", ".join(placeholders)})
    """

    with engine.connect() as connection:
        connection.execute(text(sql), records)
        connection.commit()

    return len(records)


def load_parquet_files_to_dw(
    engine: Engine,
    parquet_paths: list[str],
    table_name: str,
    schema: str | None = "public",
) -> int:
    """
    Read Parquet files, ensure DW table schema, and append data to the DW.
    Returns total rows inserted.
    """
    if not parquet_paths:
        return 0

    total_rows = 0

    for path_str in parquet_paths:
        path = Path(path_str)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path_str}")

        frame = pl.read_parquet(path)
        if frame.is_empty():
            logger.info("Skipping empty Parquet file", extra={"path": path_str})
            continue

        # Ensure schema exists in DW (idempotent)
        ensure_table_exists(engine, table_name, frame, schema=schema)

        # Add source file lineage
        frame = frame.with_columns(pl.lit(path.name).alias("_source_file"))

        # Write to database
        rows = _write_frame_to_db(
            engine=engine,
            frame=frame,
            table_name=table_name,
            schema=schema,
        )

        total_rows += rows
        logger.info(
            "Loaded Parquet to DW",
            extra={
                "path": path_str,
                "table": f"{schema}.{table_name}" if schema else table_name,
                "rows": rows,
            },
        )

    return total_rows
