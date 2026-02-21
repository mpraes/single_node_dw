"""
Mage Data Exporter: Execute complete ETL pipeline using framework CLI.

This block executes your full ETL pipeline (extract -> stage -> load)
using the existing CLI commands, perfect for simple source-to-DW workflows.
"""
from typing import Any, Dict, List
import sys
import os
from pathlib import Path

# Add ETL framework to path
sys.path.append('/app/etl')

from custom.etl_runner import execute_etl_pipeline, test_connection

@data_exporter
def run_full_etl_pipeline(
    dummy_input: Any,  # Mage requires an input, but we don't use it
    connector_config: Dict[str, Any],
    query: str,
    source_name: str,
    target_table: str,
    schema: str = "public",
    pipeline_name: str = "mage_orchestrated",
    *args, **kwargs
) -> Dict[str, Any]:
    """
    Execute a complete ETL pipeline using the framework CLI.
    
    This is the simplest integration - it runs your existing ETL pipeline
    end-to-end without intermediate Mage processing.
    
    Args:
        dummy_input: Ignored (Mage requirement)
        connector_config: Source connector configuration
        query: Query or resource to fetch from source
        source_name: Logical name of source system
        target_table: Target table name in DW
        schema: Target schema (default: public)
        pipeline_name: Pipeline name for auditing
    
    Returns:
        Dict with pipeline execution results
    """
    
    try:
        print(f"🚀 Starting full ETL pipeline: {pipeline_name}")
        print(f"   Source: {source_name} ({connector_config.get('protocol', 'unknown')})")
        print(f"   Target: {schema}.{target_table}")
        print(f"   Query: {query}")
        
        # Create temporary config file
        import tempfile
        import json
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(connector_config, f, indent=2)
            temp_config_path = f.name
        
        try:
            # Test connection first
            print(f"🔍 Testing source connection...")
            test_result = test_connection(temp_config_path)
            
            if not test_result["success"]:
                raise Exception(f"Connection test failed: {test_result.get('stderr', 'Unknown error')}")
            
            print(f"✅ Connection test passed")
            
            # Execute full pipeline
            result = execute_etl_pipeline(
                config_path=temp_config_path,
                query=query,
                source_name=source_name,
                target_table=target_table,
                schema=schema,
                pipeline_name=pipeline_name,
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
                    "pipeline_name": pipeline_name,
                    "source_name": source_name,
                    "target_table": f"{schema}.{target_table}",
                    "pipeline_result": pipeline_result,
                    "execution_summary": {
                        "rows_loaded": pipeline_result.get('rows_loaded', 0),
                        "parquet_files": pipeline_result.get('parquet_files', 0),
                        "duration_seconds": pipeline_result.get('duration_seconds', 0),
                        "run_id": pipeline_result.get('run_id')
                    }
                }
            else:
                error_msg = result.get("stderr", result.get("error", "Unknown pipeline error"))
                print(f"❌ Pipeline failed: {error_msg}")
                
                return {
                    "success": False,
                    "pipeline_name": pipeline_name,
                    "source_name": source_name,
                    "target_table": f"{schema}.{target_table}",
                    "error": error_msg,
                    "error_details": result
                }
                
        finally:
            # Clean up temporary config file
            os.unlink(temp_config_path)
            
    except Exception as e:
        print(f"💥 Pipeline execution failed: {str(e)}")
        return {
            "success": False,
            "pipeline_name": pipeline_name,
            "source_name": source_name,
            "target_table": f"{schema}.{target_table}",
            "error": str(e),
            "exception": True
        }


@test
def test_run_full_etl_pipeline(output, *args) -> None:
    """
    Test that pipeline execution completed.
    """
    assert isinstance(output, dict), "Output should be a dictionary"
    assert "success" in output, "Output should indicate success/failure"
    assert "pipeline_name" in output, "Should include pipeline name"
    assert "source_name" in output, "Should include source name"
    assert "target_table" in output, "Should include target table"
    
    if output.get("success"):
        assert "execution_summary" in output, "Successful runs should have execution summary"
        assert "run_id" in output.get("execution_summary", {}), "Should have audit run ID"
    else:
        assert "error" in output, "Failed runs should have error message"