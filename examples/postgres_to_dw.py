import os
import sys
from pathlib import Path

# Add the etl package to the path
ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = ROOT / "etl"
if str(ETL_PATH) not in sys.path:
    sys.path.insert(0, str(ETL_PATH))

from connections.dw_destination import get_dw_engine
from pipeline.runner import run_pipeline

def main():
    """
    Example: Syncing a table from a source PostgreSQL to the Data Warehouse.
    
    This example assumes you have:
    1. A source Postgres database
    2. A destination DW Postgres database
    3. Environment variables or a .env file configured
    """
    
    # Configuration for the source connector
    # You can also use a JSON file: load_connector_config("postgres_connector.json")
    connector_config = {
        "protocol": "postgres",
        "env_prefix": "PG_SOURCE" # Look for PG_SOURCE_HOST, PG_SOURCE_USER, etc.
    }
    
    # Destination DW engine
    # Look for DW_HOST, DW_USER, DW_DATABASE, etc. (the default env_prefix)
    dw_engine = get_dw_engine()
    
    print("Starting PostgreSQL to DW pipeline...")
    
    result = run_pipeline(
        connector_config=connector_config,
        query="SELECT * FROM public.users",
        source_name="postgres_prod",
        target_table="stg_users",
        lake_path="./lake",
        dw_engine=dw_engine,
        pipeline_name="postgres_sync"
    )
    
    if result["status"] == "success":
        print(f"Success! Loaded {result['rows_loaded']} rows.")
        print(f"Run ID: {result['run_id']}")
    else:
        print(f"Pipeline failed: {result.get('error')}")

if __name__ == "__main__":
    main()
