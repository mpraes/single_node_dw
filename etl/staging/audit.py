from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from connections._logging import get_logger

logger = get_logger("staging.audit")


def ensure_audit_table(engine: Engine) -> None:
    """Ensure the etl_audit_log table exists in the DW."""
    is_sqlite = engine.dialect.name == "sqlite"
    id_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"
    ts_type = "TIMESTAMP" if is_sqlite else "TIMESTAMP WITH TIME ZONE"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS etl_audit_log (
        id            {id_type},
        run_id        TEXT NOT NULL,
        pipeline_name TEXT NOT NULL,
        source_name   TEXT NOT NULL,
        protocol      TEXT NOT NULL,
        target_table  TEXT NOT NULL,
        status        TEXT NOT NULL,   -- 'success' | 'failure'
        rows_loaded   BIGINT,
        parquet_files INTEGER,
        error_message TEXT,
        started_at    {ts_type} NOT NULL,
        finished_at   {ts_type}
    )
    """
    with engine.connect() as connection:
        logger.info("Ensuring audit table exists")
        connection.execute(text(ddl))
        connection.commit()


def write_audit_record(
    engine: Engine,
    run_id: str,
    pipeline_name: str,
    source_name: str,
    protocol: str,
    target_table: str,
    status: str,
    rows_loaded: int | None,
    parquet_files: int | None,
    started_at: datetime,
    finished_at: datetime | None,
    error_message: str | None = None,
) -> None:
    """Insert or update an audit record in the DW."""
    sql = """
    INSERT INTO etl_audit_log (
        run_id, pipeline_name, source_name, protocol, target_table,
        status, rows_loaded, parquet_files, started_at, finished_at, error_message
    ) VALUES (
        :run_id, :pipeline_name, :source_name, :protocol, :target_table,
        :status, :rows_loaded, :parquet_files, :started_at, :finished_at, :error_message
    )
    """
    params = {
        "run_id": run_id,
        "pipeline_name": pipeline_name,
        "source_name": source_name,
        "protocol": protocol,
        "target_table": target_table,
        "status": status,
        "rows_loaded": rows_loaded,
        "parquet_files": parquet_files,
        "started_at": started_at,
        "finished_at": finished_at,
        "error_message": error_message,
    }

    with engine.connect() as connection:
        logger.info("Writing audit record", extra={"run_id": run_id, "status": status})
        connection.execute(text(sql), params)
        connection.commit()
