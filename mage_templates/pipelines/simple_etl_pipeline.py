"""
Mage Pipeline Example: Simple ETL (Extract -> Transform -> Load)

This pipeline demonstrates how to use Mage with your ETL framework:
1. Extract data using ETL framework connectors
2. Transform with pandas/Mage capabilities  
3. Load to DW using ETL framework

To use this pipeline:
1. Copy this file to your Mage project
2. Update the connector configuration
3. Customize the transformation logic
4. Schedule in Mage UI
"""

from mage_ai.data_preparation.decorators import data_loader, transformer, data_exporter
import pandas as pd
from typing import Dict, Any

# =============================================================================
# DATA LOADER: Extract from Source
# =============================================================================

@data_loader
def extract_from_source(*args, **kwargs) -> pd.DataFrame:
    """
    Extract data using ETL framework connector.
    """
    # Import ETL framework
    import sys
    sys.path.append('/app/etl')
    from custom.etl_runner import test_connection
    from connections.sources.factory import create_connector
    
    # Connector configuration
    connector_config = {
        "protocol": "http",
        "base_url": "https://jsonplaceholder.typicode.com"
    }
    
    query = "/users"
    
    print(f"🔗 Extracting data from {connector_config.get('protocol')} source...")
    
    # Create connector and extract
    connector = create_connector(connector_config)
    connector.connect()
    
    try:
        result = connector.fetch_data(query)
        
        if not result.success:
            raise Exception(f"Data extraction failed: {result.metadata.get('error')}")
        
        # Convert to DataFrame
        if hasattr(result, 'dataframe') and result.dataframe is not None:
            df = result.dataframe.to_pandas()
        else:
            df = pd.DataFrame(result.data)
        
        print(f"✅ Extracted {len(df)} rows")
        return df
        
    finally:
        connector.close()


# =============================================================================
# TRANSFORMER: Clean and Process Data
# =============================================================================

@transformer
def clean_and_transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """
    Clean and transform the extracted data.
    """
    print(f"🔧 Transforming {len(df)} rows...")
    
    # Example transformations
    df_clean = df.copy()
    
    # 1. Clean column names
    df_clean.columns = df_clean.columns.str.lower().str.replace(' ', '_')
    
    # 2. Handle missing values
    df_clean = df_clean.fillna('')
    
    # 3. Add audit columns
    df_clean['load_timestamp'] = pd.Timestamp.now()
    df_clean['pipeline_name'] = 'mage_simple_etl'
    
    # 4. Data type conversions
    for col in df_clean.select_dtypes(include=['object']).columns:
        if col not in ['load_timestamp']:
            df_clean[col] = df_clean[col].astype(str)
    
    print(f"✅ Transformation complete: {len(df_clean)} rows, {len(df_clean.columns)} columns")
    
    return df_clean


# =============================================================================
# DATA EXPORTER: Load to Data Warehouse
# =============================================================================

@data_exporter
def load_to_dw(df: pd.DataFrame, *args, **kwargs) -> Dict[str, Any]:
    """
    Load transformed data to the data warehouse.
    """
    import sys
    sys.path.append('/app/etl')
    from custom.etl_runner import test_dw_connection
    from connections.dw_destination import get_dw_engine
    from staging.loader import load_parquet_files_to_dw
    from staging.audit import ensure_audit_table, write_audit_record
    import polars as pl
    from pathlib import Path
    import uuid
    from datetime import datetime, UTC
    
    target_table = "stg_mage_users"
    schema = "public"
    source_name = "mage_simple_etl"
    
    print(f"🏗️ Loading {len(df)} rows to {schema}.{target_table}")
    
    # Test DW connection
    test_result = test_dw_connection()
    if not test_result["success"]:
        raise Exception(f"DW connection failed: {test_result.get('stderr')}")
    
    # Get DW engine and ensure audit table
    dw_engine = get_dw_engine()
    ensure_audit_table(dw_engine)
    
    # Convert to Polars and stage
    polars_df = pl.from_pandas(df)
    lake_path = Path("/app/lake")
    lake_path.mkdir(exist_ok=True)
    
    temp_parquet = lake_path / f"mage_simple_etl_{target_table}.parquet"
    
    try:
        # Write to parquet
        polars_df.write_parquet(temp_parquet)
        
        # Load to DW
        rows_loaded = load_parquet_files_to_dw(
            engine=dw_engine,
            parquet_paths=[str(temp_parquet)],
            table_name=target_table,
            schema=schema
        )
        
        # Audit
        run_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        
        write_audit_record(
            engine=dw_engine,
            run_id=run_id,
            pipeline_name="mage_simple_etl",
            source_name=source_name,
            protocol="mage_pipeline",
            target_table=target_table,
            status="success",
            rows_loaded=rows_loaded,
            parquet_files=1,
            started_at=now,
            finished_at=now,
            schema=schema
        )
        
        print(f"✅ Successfully loaded {rows_loaded} rows")
        
        return {
            "success": True,
            "rows_loaded": rows_loaded,
            "target_table": f"{schema}.{target_table}",
            "run_id": run_id
        }
        
    finally:
        if temp_parquet.exists():
            temp_parquet.unlink()


# =============================================================================
# TESTS
# =============================================================================

@test
def test_extract_output(output, *args) -> None:
    assert isinstance(output, pd.DataFrame)
    assert len(output) > 0
    assert len(output.columns) > 0

@test  
def test_transform_output(output, *args) -> None:
    assert isinstance(output, pd.DataFrame)
    assert 'load_timestamp' in output.columns
    assert 'pipeline_name' in output.columns

@test
def test_load_output(output, *args) -> None:
    assert isinstance(output, dict)
    assert output.get("success") == True
    assert output.get("rows_loaded", 0) > 0