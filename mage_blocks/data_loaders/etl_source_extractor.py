"""
Mage Data Loader: Extract data using ETL framework connectors.

This block uses your ETL framework's connector system to extract data
and return it as a pandas DataFrame for further Mage processing.
"""
from typing import Any, Dict, List
import pandas as pd
import sys
import os
from pathlib import Path

# Add ETL framework to path
sys.path.append('/app/etl')

from connections.sources.factory import create_connector, load_connector_config
from custom.etl_runner import test_connection

@data_loader
def extract_data_from_source(
    connector_config: Dict[str, Any],
    query: str,
    *args, **kwargs
) -> pd.DataFrame:
    """
    Extract data using ETL framework connectors and return as DataFrame.
    
    Args:
        connector_config: Connector configuration dict
        query: Query or resource identifier to fetch
    
    Returns:
        pandas DataFrame with extracted data
    """
    
    try:
        print(f"🔗 Connecting to {connector_config.get('protocol', 'unknown')} source...")
        
        # Test connection first
        # Create a temporary config file for testing
        import tempfile
        import json
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(connector_config, f)
            temp_config_path = f.name
        
        try:
            test_result = test_connection(temp_config_path)
            if not test_result["success"]:
                raise Exception(f"Connection test failed: {test_result.get('stderr', 'Unknown error')}")
            
            print(f"✅ Connection test successful")
            
            # Create connector and extract data
            connector = create_connector(connector_config)
            connector.connect()
            
            try:
                print(f"📥 Extracting data with query: {query}")
                result = connector.fetch_data(query)
                
                if not result.success:
                    raise Exception(f"Data extraction failed: {result.metadata.get('error', 'Unknown error')}")
                
                # Convert to DataFrame
                if hasattr(result, 'dataframe') and result.dataframe is not None:
                    df = result.dataframe.to_pandas()
                elif hasattr(result, 'data') and result.data:
                    df = pd.DataFrame(result.data)
                else:
                    print("⚠️ No data returned from source")
                    df = pd.DataFrame()
                
                print(f"✅ Extracted {len(df)} rows, {len(df.columns)} columns")
                
                # Add metadata columns
                df['_extraction_timestamp'] = pd.Timestamp.now()
                df['_source_protocol'] = connector_config.get('protocol', 'unknown')
                
                return df
                
            finally:
                connector.close()
                
        finally:
            # Clean up temp file
            os.unlink(temp_config_path)
            
    except Exception as e:
        print(f"❌ Data extraction failed: {str(e)}")
        raise


@test
def test_extract_data_from_source(output, *args) -> None:
    """
    Test that data extraction returns valid DataFrame.
    """
    assert isinstance(output, pd.DataFrame), "Output should be a pandas DataFrame"
    assert len(output.columns) > 0, "DataFrame should have columns"
    assert '_extraction_timestamp' in output.columns, "Should have extraction timestamp"
    assert '_source_protocol' in output.columns, "Should have source protocol info"