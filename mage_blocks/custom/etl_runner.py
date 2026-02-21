"""
Custom Mage block to run ETL pipelines using the Single Node DW framework.

This block integrates your existing ETL CLI with Mage.ai orchestration.
"""
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

def execute_etl_pipeline(
    config_path: str,
    query: str,
    source_name: str,
    target_table: str,
    schema: str = "public",
    pipeline_name: str = "mage_orchestrated",
    lake_path: str = "/app/lake"
) -> Dict[str, Any]:
    """
    Execute ETL pipeline using the framework CLI.
    
    Args:
        config_path: Path to connector configuration file
        query: Query or resource to fetch from source
        source_name: Logical name of the source system
        target_table: Target table name in DW
        schema: Target schema (default: public)
        pipeline_name: Pipeline name for auditing
        lake_path: Path for staging Parquet files
    
    Returns:
        Dict with execution results
    """
    
    # Ensure paths exist
    os.makedirs(lake_path, exist_ok=True)
    
    # Build CLI command
    cmd = [
        sys.executable, "-m", "etl.cli", "run",
        "--config", config_path,
        "--query", query,
        "--source", source_name,
        "--table", target_table,
        "--lake", lake_path,
        "--schema", schema,
        "--pipeline", pipeline_name
    ]
    
    try:
        # Execute pipeline
        print(f"🚀 Starting ETL pipeline: {pipeline_name}")
        print(f"   Source: {source_name}")
        print(f"   Target: {schema}.{target_table}")
        print(f"   Config: {config_path}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/app",
            timeout=3600  # 1 hour timeout
        )
        
        # Parse result
        if result.returncode == 0:
            # Parse JSON output from CLI
            try:
                pipeline_result = json.loads(result.stdout.split('\n')[0])
                print(f"✅ Pipeline completed successfully!")
                print(f"   Run ID: {pipeline_result.get('run_id')}")
                print(f"   Rows loaded: {pipeline_result.get('rows_loaded', 0)}")
                print(f"   Duration: {pipeline_result.get('duration_seconds', 0):.2f}s")
                
                return {
                    "success": True,
                    "pipeline_result": pipeline_result,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            except (json.JSONDecodeError, IndexError):
                print(f"✅ Pipeline completed (could not parse JSON output)")
                return {
                    "success": True,
                    "pipeline_result": {"status": "success"},
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
        else:
            print(f"❌ Pipeline failed with exit code {result.returncode}")
            print(f"   Error: {result.stderr}")
            
            return {
                "success": False,
                "error_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Pipeline timed out after 1 hour")
        return {
            "success": False,
            "error": "Pipeline execution timeout",
            "timeout": True
        }
    except Exception as e:
        print(f"💥 Unexpected error: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "exception": True
        }


def test_connection(config_path: str) -> Dict[str, Any]:
    """
    Test connection to a data source.
    
    Args:
        config_path: Path to connector configuration file
    
    Returns:
        Dict with test results
    """
    cmd = [
        sys.executable, "-m", "etl.cli", "test-connection",
        "--config", config_path
    ]
    
    try:
        print(f"🔍 Testing connection: {config_path}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/app",
            timeout=60  # 1 minute timeout for connection tests
        )
        
        if result.returncode == 0:
            try:
                test_result = json.loads(result.stdout.split('\n')[0])
                print(f"✅ Connection test successful: {test_result.get('label')}")
                return {
                    "success": True,
                    "test_result": test_result,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            except (json.JSONDecodeError, IndexError):
                print(f"✅ Connection test passed")
                return {
                    "success": True,
                    "test_result": {"success": True},
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
        else:
            print(f"❌ Connection test failed: {result.stderr}")
            return {
                "success": False,
                "error_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏰ Connection test timed out")
        return {
            "success": False,
            "error": "Connection test timeout"
        }
    except Exception as e:
        print(f"💥 Connection test error: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def test_dw_connection() -> Dict[str, Any]:
    """
    Test connection to the data warehouse.
    
    Returns:
        Dict with test results
    """
    cmd = [
        sys.executable, "-m", "etl.cli", "test-connection",
        "--source", "dw"
    ]
    
    try:
        print(f"🔍 Testing Data Warehouse connection...")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/app",
            timeout=30
        )
        
        if result.returncode == 0:
            try:
                test_result = json.loads(result.stdout.split('\n')[0])
                print(f"✅ DW connection successful")
                return {
                    "success": True,
                    "test_result": test_result,
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
            except (json.JSONDecodeError, IndexError):
                print(f"✅ DW connection successful")
                return {
                    "success": True,
                    "test_result": {"success": True},
                    "stdout": result.stdout,
                    "stderr": result.stderr
                }
        else:
            print(f"❌ DW connection failed: {result.stderr}")
            return {
                "success": False,
                "error_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
    except Exception as e:
        print(f"💥 DW connection test error: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }