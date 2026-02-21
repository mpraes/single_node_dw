import uuid
from datetime import datetime, timezone as UTC

from sqlalchemy.engine import Engine

from connections._logging import get_logger
from connections.sources.factory import create_connector
from staging import (
    ensure_audit_table,
    load_parquet_files_to_dw,
    write_audit_record,
    write_ingestion_result_to_parquet,
)

logger = get_logger("pipeline.runner")


def run_pipeline(
    connector_config: dict,
    query: str,
    source_name: str,
    target_table: str,
    lake_path: str,
    dw_engine: Engine,
    schema: str | None = "public",
    pipeline_name: str = "default",
) -> dict:
    """
    Orchestrate a full ETL pipeline run:
    1. Setup audit
    2. Fetch data from source
    3. Stage to Parquet
    4. Load to DW
    5. Log results to audit table
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC.utc)
    protocol = connector_config.get("protocol", "unknown")

    # 1. Ensure audit table exists
    ensure_audit_table(dw_engine)

    try:
        # 2. Fetch data
        connector = create_connector(connector_config)
        connector.connect()
        try:
            result = connector.fetch_data(query)
        finally:
            connector.close()

        if not result.success:
            error_msg = f"Ingestion failed: {result.metadata.get('error')}"
            write_audit_record(
                dw_engine, run_id, pipeline_name, source_name, protocol, target_table,
                "failure", 0, 0, started_at, datetime.now(UTC.utc), error_msg
            )
            return {
                "run_id": run_id,
                "status": "failure",
                "rows_loaded": 0,
                "parquet_files": 0,
                "error": error_msg
            }

        # 3. Stage to Parquet
        parquet_paths = write_ingestion_result_to_parquet(
            result=result,
            lake_path=lake_path,
            source_name=source_name,
        )

        # 4. Load to DW
        rows_loaded = load_parquet_files_to_dw(
            engine=dw_engine,
            parquet_paths=parquet_paths,
            table_name=target_table,
            schema=schema,
        )

        finished_at = datetime.now(UTC.utc)
        parquet_count = len(parquet_paths)

        # 5. Success audit
        write_audit_record(
            dw_engine, run_id, pipeline_name, source_name, protocol, target_table,
            "success", rows_loaded, parquet_count, started_at, finished_at
        )

        duration = (finished_at - started_at).total_seconds()

        return {
            "run_id": run_id,
            "status": "success",
            "rows_loaded": rows_loaded,
            "parquet_files": parquet_count,
            "duration_seconds": duration,
        }

    except Exception as e:
        logger.exception("Pipeline execution failed", extra={"run_id": run_id})
        finished_at = datetime.now(UTC.utc)
        write_audit_record(
            dw_engine, run_id, pipeline_name, source_name, protocol, target_table,
            "failure", 0, 0, started_at, finished_at, str(e)
        )
        raise
