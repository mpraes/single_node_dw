"""
Mage Data Exporter: Load data to DW using ETL framework.

This block takes a DataFrame and loads it to the data warehouse
using your ETL framework's staging and loading capabilities.
"""
from typing import Any, Dict, List
import pandas as pd
import sys
import os
from pathlib import Path
import tempfile
import json

# Add ETL framework to path
sys.path.append('/app/etl')

from connections.dw_destination import get_dw_engine
from staging import write_dataframe_to_parquet, load_parquet_files_to_dw, ensure_audit_table
from custom.etl_runner import test_dw_connection

@data_exporter
def load_data_to_dw(
    df: pd.DataFrame,
    target_table: str,
    schema: str = "public",
    source_name: str = "mage_pipeline",
    *args, **kwargs
) -> Dict[str, Any]:
    """
    Load DataFrame to data warehouse using ETL framework.
    
    Args:
        df: pandas DataFrame to load
        target_table: Target table name in DW
        schema: Target schema (default: public)
        source_name: Logical source name for auditing
    
    Returns:
        Dict with loading results
    """
    
    try:
        print(f"🏗️ Loading {len(df)} rows to {schema}.{target_table}")
        
        # Test DW connection
        test_result = test_dw_connection()
        if not test_result["success"]:
            raise Exception(f"DW connection failed: {test_result.get('stderr', 'Unknown error')}")
        
        print(f"✅ DW connection verified")
        
        # Get DW engine
        dw_engine = get_dw_engine()
        
        # Ensure audit table exists
        ensure_audit_table(dw_engine)
        
        # Convert DataFrame to Polars (ETL framework uses Polars internally)
        import polars as pl
        
        # Remove Mage metadata columns if they exist
        df_clean = df.copy()
        mage_columns = [col for col in df_clean.columns if col.startswith('_mage_')]
        if mage_columns:
            df_clean = df_clean.drop(columns=mage_columns)
            print(f"📝 Removed Mage metadata columns: {mage_columns}")
        
        # Convert to Polars
        polars_df = pl.from_pandas(df_clean)
        
        # Create temporary parquet file
        lake_path = Path("/app/lake")
        lake_path.mkdir(exist_ok=True)
        
        temp_parquet = lake_path / f"temp_mage_{target_table}.parquet"
        
        try:
            # Write to parquet
            polars_df.write_parquet(temp_parquet)
            print(f"💾 Staged data to: {temp_parquet}")
            
            # Load to DW
            from staging.loader import load_parquet_files_to_dw
            
            rows_loaded = load_parquet_files_to_dw(
                engine=dw_engine,
                parquet_paths=[str(temp_parquet)],
                table_name=target_table,
                schema=schema
            )
            
            print(f"✅ Successfully loaded {rows_loaded} rows to {schema}.{target_table}")
            
            # Write audit record
            from staging.audit import write_audit_record
            from datetime import datetime, UTC
            import uuid
            
            run_id = str(uuid.uuid4())
            now = datetime.now(UTC)
            
            write_audit_record(
                engine=dw_engine,
                run_id=run_id,
                pipeline_name="mage_orchestrated",
                source_name=source_name,
                protocol="mage_dataframe",
                target_table=target_table,
                status="success",
                rows_loaded=rows_loaded,
                parquet_files=1,
                started_at=now,
                finished_at=now,
                schema=schema
            )
            
            return {
                "success": True,
                "rows_loaded": rows_loaded,
                "target_table": f"{schema}.{target_table}",
                "run_id": run_id,
                "parquet_file": str(temp_parquet)
            }
            
        finally:
            # Clean up temporary parquet file
            if temp_parquet.exists():
                temp_parquet.unlink()
                print(f"🧹 Cleaned up temporary file: {temp_parquet}")
            
    except Exception as e:
        print(f"❌ Data loading failed: {str(e)}")
        
        # Write failure audit record
        try:
            dw_engine = get_dw_engine()
            from staging.audit import write_audit_record
            from datetime import datetime, UTC
            import uuid
            
            run_id = str(uuid.uuid4())
            now = datetime.now(UTC)
            
            write_audit_record(
                engine=dw_engine,
                run_id=run_id,
                pipeline_name="mage_orchestrated",
                source_name=source_name,
                protocol="mage_dataframe",
                target_table=target_table,
                status="failure",
                rows_loaded=0,
                parquet_files=0,
                started_at=now,
                finished_at=now,
                error_message=str(e),
                schema=schema
            )
        except:
            pass  # Don't fail if audit logging fails
        
        raise


@test
def test_load_data_to_dw(output, *args) -> None:
    """
    Test that data loading completed successfully.
    """
    assert isinstance(output, dict), "Output should be a dictionary"
    assert output.get("success") == True, "Loading should be successful"
    assert output.get("rows_loaded", 0) > 0, "Should have loaded some rows"
    assert "target_table" in output, "Should specify target table"
    assert "run_id" in output, "Should have audit run ID"