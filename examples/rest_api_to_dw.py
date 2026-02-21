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
    Example: Syncing data from a REST API to the Data Warehouse.
    
    Demonstrates:
    - Connecting to an HTTP source
    - Nested JSON handling (flattened by the staging writer)
    - Loading into a DW table
    """
    
    # Configuration for a public mock API
    connector_config = {
        "protocol": "http",
        "base_url": "https://jsonplaceholder.typicode.com",
    }
    
    # Destination DW engine
    dw_engine = get_dw_engine()
    
    print("Starting REST API to DW pipeline...")
    
    # query for HTTP connector is the relative URL/endpoint
    result = run_pipeline(
        connector_config=connector_config,
        query="/users",
        source_name="placeholder_api",
        target_table="stg_api_users",
        lake_path="./lake",
        dw_engine=dw_engine,
        pipeline_name="api_sync"
    )
    
    if result["status"] == "success":
        print(f"Success! Loaded {result['rows_loaded']} rows.")
        print(f"Run ID: {result['run_id']}")
    else:
        print(f"Pipeline failed: {result.get('error')}")

if __name__ == "__main__":
    main()
