import json
import sys
from pathlib import Path
from datetime import UTC, datetime

# Add the etl package to the path
ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = ROOT / "etl"
if str(ETL_PATH) not in sys.path:
    sys.path.insert(0, str(ETL_PATH))

from connections.dw_destination import get_dw_engine
from connections.sources.sql.postgres import get_postgres_engine
from connections.sources.sql.incremental import fetch_incremental_rows
from connections.sources.data_contract import IngestionResult, IngestedItem
from staging import write_ingestion_result_to_parquet, load_parquet_files_to_dw, write_audit_record, ensure_audit_table

def main():
    """
    Example: Incremental sync from PostgreSQL to the Data Warehouse.
    Uses a local JSON file to persist the watermark (last ID seen).
    """
    watermark_file = Path("watermark.json")
    last_id = 0
    if watermark_file.exists():
        last_id = json.loads(watermark_file.read_text()).get("last_id", 0)

    print(f"Starting incremental sync from ID > {last_id}")

    # 1. Setup engines
    source_engine = get_postgres_engine(env_prefix="PG_SOURCE")
    dw_engine = get_dw_engine()
    ensure_audit_table(dw_engine)
    
    run_id = f"inc-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    start_time = datetime.now(UTC)

    try:
        # 2. Fetch incremental rows
        rows, new_watermark = fetch_incremental_rows(
            engine=source_engine,
            table_name="events",
            watermark_column="id",
            last_watermark=last_id,
            batch_size=1000
        )

        if not rows:
            print("No new rows found.")
            return

        # 3. Bridge to IngestionResult (manual bridge because we used fetch_incremental_rows directly)
        result = IngestionResult(
            protocol="postgres",
            success=True,
            items=[IngestedItem(payload=rows)]
        )

        # 4. Stage to Parquet
        paths = write_ingestion_result_to_parquet(result, "./lake", "events")

        # 5. Load to DW
        rows_loaded = load_parquet_files_to_dw(dw_engine, paths, "stg_events")

        # 6. Success: Update watermark and audit
        watermark_file.write_text(json.dumps({"last_id": new_watermark}))
        
        write_audit_record(
            dw_engine, run_id, "incremental_sync", "postgres_events", "postgres", 
            "stg_events", "success", rows_loaded, len(paths), start_time, datetime.now(UTC)
        )
        
        print(f"Successfully loaded {rows_loaded} new rows. New watermark: {new_watermark}")

    except Exception as e:
        print(f"Incremental sync failed: {e}")
        write_audit_record(
            dw_engine, run_id, "incremental_sync", "postgres_events", "postgres", 
            "stg_events", "failure", 0, 0, start_time, datetime.now(UTC), str(e)
        )

if __name__ == "__main__":
    main()
