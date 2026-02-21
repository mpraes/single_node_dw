import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = ROOT / "etl"
if str(ETL_PATH) not in sys.path:
    sys.path.insert(0, str(ETL_PATH))

from connections.sources.data_contract import IngestedItem, IngestionResult  # noqa: E402
from pipeline.runner import run_pipeline  # noqa: E402


def test_run_pipeline_happy_path(tmp_path):
    # Mock DW engine using SQLite
    dw_engine = create_engine("sqlite:///:memory:")
    
    # Mock connector and its result
    mock_result = IngestionResult(
        protocol="http",
        success=True,
        items=[IngestedItem(payload={"id": 1, "val": "a"})]
    )
    
    connector_config = {"protocol": "http", "base_url": "http://api"}
    
    with patch("pipeline.runner.create_connector") as mock_create:
        mock_connector = mock_create.return_value
        mock_connector.fetch_data.return_value = mock_result
        
        # We need to patch write_ingestion_result_to_parquet to return a real file
        # because load_parquet_files_to_dw checks if it exists
        parquet_file = tmp_path / "test.parquet"
        import polars as pl
        pl.DataFrame({"id": [1], "val": ["a"]}).write_parquet(parquet_file)
        
        with patch("pipeline.runner.write_ingestion_result_to_parquet", return_value=[str(parquet_file)]):
            res = run_pipeline(
                connector_config=connector_config,
                query="/data",
                source_name="api_src",
                target_table="dw_api",
                lake_path=str(tmp_path),
                dw_engine=dw_engine,
                schema=None
            )

    assert res["status"] == "success"
    assert res["rows_loaded"] == 1
    assert res["parquet_files"] == 1
    
    # Verify audit record
    with dw_engine.connect() as conn:
        audit = conn.execute(text("SELECT status, target_table FROM etl_audit_log")).fetchone()
        assert audit[0] == "success"
        assert audit[1] == "dw_api"
        
        # Verify data in DW
        data = conn.execute(text("SELECT id, val, _source_file FROM dw_api")).fetchone()
        assert data[0] == 1
        assert data[1] == "a"
        assert data[2] == "test.parquet"


def test_run_pipeline_ingestion_failure():
    dw_engine = create_engine("sqlite:///:memory:")
    mock_result = IngestionResult(protocol="http", success=False, metadata={"error": "404"})
    
    connector_config = {"protocol": "http"}
    
    with patch("pipeline.runner.create_connector") as mock_create:
        mock_connector = mock_create.return_value
        mock_connector.fetch_data.return_value = mock_result
        
        res = run_pipeline(
            connector_config=connector_config,
            query="/data",
            source_name="api_src",
            target_table="dw_api",
            lake_path="/tmp",
            dw_engine=dw_engine
        )

    assert res["status"] == "failure"
    assert "Ingestion failed" in res["error"]
    
    with dw_engine.connect() as conn:
        audit = conn.execute(text("SELECT status, error_message FROM etl_audit_log")).fetchone()
        assert audit[0] == "failure"
        assert "404" in audit[1]


def test_run_pipeline_exception_propagates():
    dw_engine = create_engine("sqlite:///:memory:")
    
    with patch("pipeline.runner.create_connector", side_effect=RuntimeError("Connect error")):
        import pytest
        with pytest.raises(RuntimeError) as excinfo:
            run_pipeline(
                connector_config={"protocol": "http"},
                query="/data",
                source_name="api_src",
                target_table="dw_api",
                lake_path="/tmp",
                dw_engine=dw_engine
            )
        assert "Connect error" in str(excinfo.value)
    
    with dw_engine.connect() as conn:
        audit = conn.execute(text("SELECT status, error_message FROM etl_audit_log")).fetchone()
        assert audit[0] == "failure"
        assert "Connect error" in audit[1]
