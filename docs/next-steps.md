# Single Node DW - Next Steps Plan

## Context

The project goal is a "quick and no effort DW inside a VM." The extraction layer (15+ connectors, factory, data contract, config system, caching, logging) is complete and well-tested. The missing pieces are everything downstream: staging (Parquet), DW schema management, loading into PostgreSQL, audit/lineage, pipeline orchestration, a CLI, and examples. Without these, the connectors produce `IngestionResult` objects with no automated path into the DW.

---

## New Directory Structure

```
etl/
├── __init__.py                  # NEW
├── cli.py                       # NEW
├── connections/                 # EXISTING (unchanged)
├── staging/                     # NEW package
│   ├── __init__.py
│   ├── writer.py                # IngestionResult → Parquet
│   ├── dw_schema.py             # PostgreSQL DDL (CREATE TABLE IF NOT EXISTS)
│   ├── loader.py                # Parquet → PostgreSQL DW
│   └── audit.py                 # etl_audit_log table
└── pipeline/                    # NEW package
    ├── __init__.py
    └── runner.py                # run_pipeline() orchestrator

examples/                        # NEW
├── postgres_to_dw.py
├── rest_api_to_dw.py
└── incremental_postgres_to_dw.py

tests/
├── test_connections.py          # EXISTING
├── test_staging.py              # NEW
├── test_pipeline.py             # NEW
└── test_cli.py                  # NEW

docs/guides/pipeline.md          # NEW
Makefile                         # MODIFIED
mkdocs.yml                       # MODIFIED
```

---

## Design Constraints (from AGENTS.md + codebase)

- Functions, not classes
- Clear, positional arguments — no `*args`/`**kwargs`
- Follow `_batch.py` for Parquet writes (polars, UTC timestamps, partition paths)
- Use `get_logger` from `connections._logging`
- Tests: `unittest.TestCase`, `tempfile.TemporaryDirectory`, mock only externals, no live services
- No new dependencies — use polars, sqlalchemy, psycopg, pydantic, stdlib only

---

## Critical Reference Files

| File | Purpose |
|------|---------|
| `etl/connections/sources/streams/_batch.py` | Pattern for Parquet staging writer |
| `etl/connections/sources/data_contract.py` | `IngestionResult` / `IngestedItem` models |
| `etl/connections/dw_destination.py` | `get_dw_engine()` pattern for the loader and audit |
| `tests/test_connections.py` | Test conventions to match exactly |
| `Makefile` | Existing target format for new targets |

---

## Tasks (ordered by dependency)

### Phase 1: Staging Layer

#### Task 1 — `etl/staging/writer.py`
Function `write_ingestion_result_to_parquet(result, lake_path, source_name) -> list[str]`

- Each `IngestedItem.payload` (dict or list[dict]) → rows in a Parquet file
- Partition path: `<lake_path>/<protocol>/<source_name>/<YYYY-MM-DD>/`
- Filename: `<source_name>_<utc_timestamp>.parquet`
- Items already with `lake_path` set (stream connectors) → skip write, return existing path
- Return `[]` if `result.success is False` or no items
- Log every file written

#### Task 2 — `etl/staging/__init__.py`
Export `write_ingestion_result_to_parquet`.

#### Task 3 — `tests/test_staging.py` (staging writer tests)
- dict payload → one Parquet file, correct rows
- list-of-dicts payload → correct row count
- `success=False` → returns `[]`, writes nothing
- Empty items → returns `[]`
- Items with `lake_path` already set → skips write, returns existing path
- Correct partition path structure

---

### Phase 2: DW Schema Management

#### Task 4 — `etl/staging/dw_schema.py`
Function `ensure_table_exists(engine, table_name, frame, schema="public") -> None`

- `CREATE TABLE IF NOT EXISTS` with columns inferred from `pl.DataFrame` schema
- Type mapping: `Utf8/String` → `TEXT`, `Int32/Int64` → `BIGINT`, `Float*` → `DOUBLE PRECISION`, `Boolean` → `BOOLEAN`, `Date` → `DATE`, `Datetime` → `TIMESTAMP WITH TIME ZONE`, else → `TEXT`
- Adds audit columns: `_loaded_at TIMESTAMP WITH TIME ZONE DEFAULT now()`, `_source_file TEXT`
- Log the DDL executed

#### Task 5 — Append schema tests to `tests/test_staging.py`
- `ensure_table_exists` with SQLite in-memory engine creates table
- Idempotent: second call does not raise
- Type mapping covers all supported Polars types
- Audit columns present

---

### Phase 3: DW Loader

#### Task 6 — `etl/staging/loader.py`
Function `load_parquet_files_to_dw(engine, parquet_paths, table_name, schema="public") -> int`

- For each path: read with polars, call `ensure_table_exists` (idempotent), add `_source_file` column, append to DW table via polars `write_database` or `to_pandas().to_sql(if_exists="append")`
- Log each file + row count
- Return total rows inserted
- Raise `FileNotFoundError` with clear message if a path is missing

#### Task 7 — Append loader tests to `tests/test_staging.py`
- Single file → correct row count in DW
- Multiple files → cumulative row count
- Empty list → returns 0
- Missing file → `FileNotFoundError`

---

### Phase 4: Audit / Lineage

#### Task 8 — `etl/staging/audit.py`

Audit table DDL (created on first use):

```sql
CREATE TABLE IF NOT EXISTS etl_audit_log (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT NOT NULL,
    pipeline_name TEXT NOT NULL,
    source_name   TEXT NOT NULL,
    protocol      TEXT NOT NULL,
    target_table  TEXT NOT NULL,
    status        TEXT NOT NULL,   -- 'success' | 'failure'
    rows_loaded   BIGINT,
    parquet_files INTEGER,
    error_message TEXT,
    started_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at   TIMESTAMP WITH TIME ZONE
)
```

Functions:

- `ensure_audit_table(engine) -> None`
- `write_audit_record(engine, run_id, pipeline_name, source_name, protocol, target_table, status, rows_loaded, parquet_files, started_at, finished_at, error_message=None) -> None`

#### Task 9 — `tests/test_staging.py` (audit tests)
- `ensure_audit_table` is idempotent
- `write_audit_record` inserts one row with correct values (SQLite in-memory)

---

### Phase 5: Pipeline Orchestrator

#### Task 10 — `etl/pipeline/runner.py`

```python
def run_pipeline(
    connector_config: dict,
    query: str,
    source_name: str,
    target_table: str,
    lake_path: str,
    dw_engine: Engine,
    schema: str = "public",
    pipeline_name: str = "default",
) -> dict:
```

Flow:

1. `run_id = str(uuid.uuid4())`, `started_at = datetime.now(UTC)`
2. `ensure_audit_table(dw_engine)`
3. `connector = create_connector(connector_config)`
4. `connector.connect()` → `result = connector.fetch_data(query)` → `connector.close()`
5. If `result.success is False` → write failure audit → return early
6. `parquet_paths = write_ingestion_result_to_parquet(...)`
7. `rows_loaded = load_parquet_files_to_dw(...)`
8. `finished_at = datetime.now(UTC)`, write success audit
9. Return `{"run_id", "status", "rows_loaded", "parquet_files", "duration_seconds"}`

Wrap steps 3–7 in `try/except`: on exception, write failure audit then re-raise.

#### Task 11 — `etl/pipeline/__init__.py`
Export `run_pipeline`.

#### Task 12 — `tests/test_pipeline.py`
- Happy path with mocked connector → audit record written, correct row count
- `result.success=False` → audit failure, early return
- Connector raises → audit failure with error_message, exception propagates
- Return dict has all expected keys

---

### Phase 6: CLI

#### Task 13 — `etl/cli.py`
`argparse`-based CLI (stdlib only, no click/typer):

```
python -m etl.cli run \
  --config connector.json \
  --query "SELECT * FROM orders" \
  --source orders \
  --table dw_orders \
  --lake ./lake \
  [--schema public] \
  [--pipeline my_pipeline]

python -m etl.cli test-connection --source dw
python -m etl.cli test-connection --config connector.json
```

- Output: JSON to stdout, human summary to stderr
- Exit code: 0 success, 1 failure

#### Task 14 — `etl/__init__.py`
Create (can be empty) to make `etl` a proper package for `python -m etl.cli`.

#### Task 15 — `tests/test_cli.py`
- `cmd_run` with mocked `run_pipeline` → correct JSON on stdout
- `cmd_test_connection` success → exit 0
- Missing required args → exit 2

---

### Phase 7: Examples

#### Task 16 — `examples/postgres_to_dw.py`
Minimal end-to-end: env vars → `run_pipeline({"protocol": "postgres", "env_prefix": "PG"}, ...)`.

#### Task 17 — `examples/rest_api_to_dw.py`
REST API → staging → DW. Shows nested JSON handling.

#### Task 18 — `examples/incremental_postgres_to_dw.py`
Uses existing `fetch_incremental_rows` + manual `IngestionResult` bridge + watermark persisted to JSON file.

---

### Phase 8: Makefile & Docs

#### Task 19 — Update `Makefile`

New targets:

```makefile
infra-up:
	docker compose up -d

infra-down:
	docker compose down

infra-status:
	docker compose ps

make-tests-staging:
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_staging.py

make-tests-pipeline:
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/test_pipeline.py

make-tests-all:
	uv run --with pytest --with-requirements etl/requirements.txt pytest -q tests/

run-pipeline:
	uv run --with-requirements etl/requirements.txt python -m etl.cli run \
		--config $(CONFIG) --query "$(QUERY)" --source $(SOURCE) \
		--table $(TABLE) --lake ./lake

test-dw:
	uv run --with-requirements etl/requirements.txt python -m etl.cli test-connection --source dw
```

#### Task 20 — `docs/guides/pipeline.md`
Full pipeline usage guide: env vars, `make infra-up`, run a pipeline, CLI reference, audit log queries.

#### Task 21 — Update `mkdocs.yml`
Add `Pipeline Runner: guides/pipeline.md` to navigation.

---

## Verification

After implementation, end-to-end validation:

1. `make infra-up` → containers running
2. Configure `.env` from `.env.example`
3. `make make-tests-all` → all tests pass
4. `make test-dw` → DW connection healthy
5. `make run-pipeline CONFIG=examples/postgres_connector.json QUERY="SELECT 1 AS n" SOURCE=test TABLE=dw_test LAKE=./lake` → exit 0, JSON result
6. Query `etl_audit_log` in PostgreSQL → one row, status=success
7. `make docs-serve` → pipeline guide visible in browser
