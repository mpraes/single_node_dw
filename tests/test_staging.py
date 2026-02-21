import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
ETL_PATH = ROOT / "etl"
if str(ETL_PATH) not in sys.path:
	sys.path.insert(0, str(ETL_PATH))

from connections.sources.data_contract import IngestedItem, IngestionResult  # noqa: E402
from staging.dw_schema import (  # noqa: E402
	_polars_dtype_to_postgres,
	_quote_identifier,
	ensure_table_exists,
)
from staging.loader import load_parquet_files_to_dw  # noqa: E402
from staging.writer import write_ingestion_result_to_parquet  # noqa: E402
from staging.audit import ensure_audit_table, write_audit_record  # noqa: E402
from unittest.mock import MagicMock, patch


def test_write_ingestion_result_with_dict_payload(tmp_path):
	result = IngestionResult(
		protocol="http",
		success=True,
		items=[IngestedItem(payload={"id": 1, "name": "alice"})],
	)

	paths = write_ingestion_result_to_parquet(
		result=result,
		lake_path=str(tmp_path),
		source_name="users",
	)

	assert len(paths) == 1
	parquet_path = Path(paths[0])
	assert parquet_path.exists()
	assert parquet_path.suffix == ".parquet"
	assert parquet_path.parent.name == datetime.now(UTC).strftime("%Y-%m-%d")
	assert parquet_path.parent.parent.name == "users"
	assert parquet_path.parent.parent.parent.name == "http"

	frame = pl.read_parquet(parquet_path)
	assert frame.height == 1
	assert "_ingested_at" in frame.columns


def test_write_ingestion_result_with_list_payload(tmp_path):
	result = IngestionResult(
		protocol="ftp",
		success=True,
		items=[IngestedItem(payload=[{"id": 1}, {"id": 2}, {"id": 3}])],
	)

	paths = write_ingestion_result_to_parquet(
		result=result,
		lake_path=str(tmp_path),
		source_name="events",
	)

	assert len(paths) == 1
	frame = pl.read_parquet(paths[0])
	assert frame.height == 3
	assert "_ingested_at" in frame.columns


def test_write_ingestion_result_returns_empty_when_failure_or_empty_items(tmp_path):
	failed = IngestionResult(protocol="kafka", success=False, items=[IngestedItem(payload={"a": 1})])
	empty = IngestionResult(protocol="kafka", success=True, items=[])

	assert write_ingestion_result_to_parquet(failed, str(tmp_path), "topic") == []
	assert write_ingestion_result_to_parquet(empty, str(tmp_path), "topic") == []
	assert list(tmp_path.rglob("*.parquet")) == []


def test_write_ingestion_result_skips_items_with_existing_lake_path(tmp_path):
	existing = str(tmp_path / "streams" / "events_20260101T000000000000Z.parquet")
	result = IngestionResult(
		protocol="nats",
		success=True,
		items=[IngestedItem(lake_path=existing, payload={"id": 1})],
	)

	paths = write_ingestion_result_to_parquet(
		result=result,
		lake_path=str(tmp_path),
		source_name="events",
	)

	assert paths == [existing]
	assert list(tmp_path.rglob("*.parquet")) == []


def test_quote_identifier():
	assert _quote_identifier("users") == '"users"'
	assert _quote_identifier('user "name"') == '"user ""name"""'


def test_polars_dtype_to_postgres():
	assert _polars_dtype_to_postgres(pl.String) == "TEXT"
	assert _polars_dtype_to_postgres(pl.Int64) == "BIGINT"
	assert _polars_dtype_to_postgres(pl.Float64) == "DOUBLE PRECISION"
	assert _polars_dtype_to_postgres(pl.Boolean) == "BOOLEAN"
	assert _polars_dtype_to_postgres(pl.Date) == "DATE"
	assert _polars_dtype_to_postgres(pl.Datetime) == "TIMESTAMP WITH TIME ZONE"
	assert _polars_dtype_to_postgres(pl.List(pl.Int64)) == "TEXT"


def test_ensure_table_exists_creates_new_table():
	engine = MagicMock()
	connection = engine.connect.return_value.__enter__.return_value

	# Mock _table_exists to return False initially
	with patch("staging.dw_schema._table_exists", return_value=False):
		frame = pl.DataFrame({"id": [1], "name": ["alice"]})
		ensure_table_exists(engine, "users", frame)

	# Verify CREATE TABLE was called
	calls = [
		call
		for call in connection.execute.call_args_list
		if "CREATE TABLE" in str(call.args[0])
	]
	assert len(calls) > 0
	ddl = str(calls[0].args[0])
	assert '"id" BIGINT' in ddl
	assert '"name" TEXT' in ddl
	assert '"users"' in ddl


def test_ensure_table_exists_adds_missing_columns():
	engine = MagicMock()
	connection = engine.connect.return_value.__enter__.return_value

	# Mock _table_exists to return True
	# Mock _existing_columns to return only "id"
	with patch("staging.dw_schema._table_exists", return_value=True), patch(
		"staging.dw_schema._existing_columns", return_value={"id", "_loaded_at", "_source_file"}
	):
		frame = pl.DataFrame({"id": [1], "new_col": [1.5]})
		ensure_table_exists(engine, "users", frame)

	# Verify ALTER TABLE was called for new_col
	calls = [
		call
		for call in connection.execute.call_args_list
		if "ALTER TABLE" in str(call.args[0])
	]
	assert len(calls) == 1
	ddl = str(calls[0].args[0])
	assert 'ADD COLUMN "new_col" DOUBLE PRECISION' in ddl


import pytest  # noqa: E402


def test_load_parquet_files_to_dw_single_file(tmp_path):
	path = tmp_path / "data.parquet"
	df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
	df.write_parquet(path)

	engine = MagicMock()
	connection = engine.connect.return_value.__enter__.return_value
	with patch("staging.loader.ensure_table_exists") as mock_ensure:
		count = load_parquet_files_to_dw(engine, [str(path)], "test_table")

	assert count == 2
	mock_ensure.assert_called_once()
	
	# Verify SQLAlchemy insert was called
	calls = [call for call in connection.execute.call_args_list if "INSERT INTO" in str(call.args[0])]
	assert len(calls) == 1
	sql = str(calls[0].args[0])
	assert '"test_table"' in sql
	assert '"a"' in sql
	assert '"b"' in sql


def test_load_parquet_files_to_dw_multiple_files(tmp_path):
	path1 = tmp_path / "1.parquet"
	path2 = tmp_path / "2.parquet"
	pl.DataFrame({"a": [1]}).write_parquet(path1)
	pl.DataFrame({"a": [2, 3]}).write_parquet(path2)

	engine = MagicMock()
	with patch("staging.loader._write_frame_to_db") as mock_write, patch("staging.loader.ensure_table_exists"):
		mock_write.side_effect = [1, 2]
		count = load_parquet_files_to_dw(engine, [str(path1), str(path2)], "test_table")

	assert count == 3


def test_load_parquet_files_to_dw_empty_list():
	engine = MagicMock()
	assert load_parquet_files_to_dw(engine, [], "test_table") == 0


def test_load_parquet_files_to_dw_file_not_found():
	engine = MagicMock()
	with pytest.raises(FileNotFoundError):
		load_parquet_files_to_dw(engine, ["non_existent.parquet"], "test_table")


def test_ensure_audit_table_is_idempotent():
	# Use SQLite for actual DDL test
	from sqlalchemy import create_engine
	engine = create_engine("sqlite:///:memory:")

	# First call creates
	ensure_audit_table(engine)

	# Check table exists
	with engine.connect() as conn:
		res = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='etl_audit_log'"))
		assert res.scalar() == "etl_audit_log"

	# Second call shouldn't fail
	ensure_audit_table(engine)


def test_write_audit_record():
	from sqlalchemy import create_engine
	engine = create_engine("sqlite:///:memory:")
	ensure_audit_table(engine)

	start = datetime(2026, 2, 21, 10, 0, 0)
	write_audit_record(
		engine=engine,
		run_id="run-123",
		pipeline_name="test-pipe",
		source_name="src",
		protocol="http",
		target_table="dest",
		status="success",
		rows_loaded=100,
		parquet_files=1,
		started_at=start,
		finished_at=None,
	)

	with engine.connect() as conn:
		res = conn.execute(text("SELECT run_id, status, rows_loaded FROM etl_audit_log")).fetchone()
		assert res[0] == "run-123"
		assert res[1] == "success"
		assert res[2] == 100
