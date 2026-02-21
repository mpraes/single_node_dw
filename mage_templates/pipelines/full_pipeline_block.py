"""
Mage Pipeline Example: Full ETL Pipeline Block

This pipeline uses the complete ETL CLI command through Mage,
perfect for simple source-to-DW workflows without intermediate processing.

To use this pipeline:
1. Copy this file to your Mage project  
2. Update the connector configuration and query
3. Schedule in Mage UI
"""

from mage_ai.data_preparation.decorators import data_loader, data_exporter
from typing import Dict, Any

# =============================================================================
# DATA LOADER: Dummy trigger (Mage requirement)
# =============================================================================

@data_loader
def pipeline_trigger(*args, **kwargs) -> Dict[str, str]:
    """
    Simple trigger to start the pipeline.
    Returns configuration for the ETL pipeline.
    """
    return {
        "status": "ready",
        "pipeline_name": "mage_full_pipeline",
        "timestamp": str(pd.Timestamp.now())
    }


# =============================================================================
# DATA EXPORTER: Execute Full ETL Pipeline
# =============================================================================

@data_exporter
def execute_full_etl_pipeline(trigger_data: Dict[str, str], *args, **kwargs) -> Dict[str, Any]:
    """
    Execute complete ETL pipeline using the framework CLI.
    """
    import sys
    sys.path.append('/app/etl')
    from custom.etl_runner import execute_etl_pipeline, test_connection
    import tempfile
    import json
    import os
    
    # Pipeline configuration
    connector_config = {
        "protocol": "http",
        "base_url": "https://jsonplaceholder.typicode.com",
        "timeout_seconds": 30
    }
    
    pipeline_params = {
        "query": "/posts",
        "source_name": "jsonplaceholder_api", 
        "target_table": "stg_api_posts",
        "schema": "public",
        "pipeline_name": "mage_full_pipeline"
    }
    
    print(f"🚀 Starting full ETL pipeline: {pipeline_params['pipeline_name']}")
    print(f"   Source: {pipeline_params['source_name']} ({connector_config.get('protocol')})")
    print(f"   Target: {pipeline_params['schema']}.{pipeline_params['target_table']}")
    print(f"   Query: {pipeline_params['query']}")
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(connector_config, f, indent=2)
        temp_config_path = f.name
    
    try:
        # Test connection
        print(f"🔍 Testing source connection...")
        test_result = test_connection(temp_config_path)
        
        if not test_result["success"]:
            raise Exception(f"Connection test failed: {test_result.get('stderr', 'Unknown error')}")
        
        print(f"✅ Connection test passed")
        
        # Execute full pipeline
        result = execute_etl_pipeline(
            config_path=temp_config_path,
            **pipeline_params,
            lake_path="/app/lake"
        )
        
        if result["success"]:
            pipeline_result = result.get("pipeline_result", {})
            print(f"🎉 Pipeline completed successfully!")
            print(f"   Run ID: {pipeline_result.get('run_id')}")
            print(f"   Rows loaded: {pipeline_result.get('rows_loaded', 0)}")
            print(f"   Duration: {pipeline_result.get('duration_seconds', 0):.2f}s")
            
            return {
                "success": True,
                "pipeline_name": pipeline_params['pipeline_name'],
                "source_name": pipeline_params['source_name'],
                "target_table": f"{pipeline_params['schema']}.{pipeline_params['target_table']}",
                "execution_summary": {
                    "rows_loaded": pipeline_result.get('rows_loaded', 0),
                    "parquet_files": pipeline_result.get('parquet_files', 0),
                    "duration_seconds": pipeline_result.get('duration_seconds', 0),
                    "run_id": pipeline_result.get('run_id')
                },
                "trigger_data": trigger_data
            }
        else:
            error_msg = result.get("stderr", result.get("error", "Unknown pipeline error"))
            print(f"❌ Pipeline failed: {error_msg}")
            raise Exception(f"ETL pipeline failed: {error_msg}")
            
    finally:
        # Clean up
        os.unlink(temp_config_path)


# =============================================================================
# TESTS
# =============================================================================

@test
def test_trigger_output(output, *args) -> None:
    assert isinstance(output, dict)
    assert "status" in output
    assert output["status"] == "ready"

@test
def test_pipeline_output(output, *args) -> None:
    assert isinstance(output, dict)
    assert output.get("success") == True
    assert "execution_summary" in output
    assert output["execution_summary"]["rows_loaded"] > 0