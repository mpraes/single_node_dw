# Azure ETL Framework — Build Prompts

> Derived from the `single_node_dw` project. The connector layer, data contract, factory pattern, incremental
> loading logic, and audit schema are all reused. What changes is the **runtime environment** and the
> **storage/database layer**: local filesystem → ADLS Gen2, PostgreSQL → Azure SQL Database,
> VM process → Azure Functions, Mage.ai → Azure Data Factory.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Azure Data Factory                               │
│   (Pipeline → Web Activity → Function App)                              │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ HTTP trigger
                ┌────────────────▼───────────────────┐
                │         Azure Function App          │
                │   run_pipeline(connector_config,    │
                │   query, source, target_table)      │
                └──────┬──────────────────┬───────────┘
                       │                  │
          ┌────────────▼──────┐    ┌──────▼───────────────────┐
          │  Source Connectors │    │  Azure Key Vault          │
          │  (reused as-is)   │    │  (secrets / conn strings) │
          └────────────┬──────┘    └──────────────────────────┘
                       │
          ┌────────────▼──────────────────────────────────────┐
          │          ADLS Gen2 – Staging Layer                 │
          │  abfs://container/protocol/source/YYYY-MM-DD/*.parquet │
          └────────────┬──────────────────────────────────────┘
                       │
          ┌────────────▼──────────────────────────────────────┐
          │          Azure SQL Database – DW                   │
          │  schema.table + etl_audit_log                      │
          └───────────────────────────────────────────────────┘
```

**Key differences from single_node_dw:**

| Concern          | single_node_dw               | azure-etl                               |
|------------------|------------------------------|-----------------------------------------|
| Compute          | VM process / Mage.ai         | Azure Function App (HTTP trigger)       |
| Orchestration    | Mage.ai                      | Azure Data Factory                      |
| Staging storage  | Local filesystem (`lake/`)   | ADLS Gen2 (azure-storage-file-datalake) |
| DW database      | PostgreSQL                   | Azure SQL Database (pyodbc + sqlalchemy)|
| Secrets          | `.env` file                  | Azure Key Vault + App Settings          |
| Parquet writer   | `polars.write_parquet(path)` | Upload bytes to ADLS blob               |
| Infra            | Terraform (optional)         | Bicep (recommended) or Terraform        |

---

## Task 1 — Repository Structure

**Prompt:**
```
Create the folder structure for a new Python ETL framework that runs on Azure Functions
and uses Azure Data Lake Storage Gen2 as the staging layer and Azure SQL Database as
the destination. The project must mirror the connector architecture from single_node_dw
(BaseConnector, factory, data_contract, IngestionResult) but replace local filesystem
I/O with ADLS Gen2 uploads and replace PostgreSQL with Azure SQL Database.

Use this structure:

azure_etl/
├── etl/
│   ├── __init__.py
│   ├── connections/          # reuse as-is from single_node_dw
│   │   ├── _config.py
│   │   ├── _engine_cache.py
│   │   ├── _logging.py
│   │   ├── _session_cache.py
│   │   ├── sources/
│   │   │   ├── base_connector.py
│   │   │   ├── data_contract.py
│   │   │   ├── factory.py
│   │   │   ├── sql/
│   │   │   ├── http/
│   │   │   ├── ftp/
│   │   │   └── ...
│   ├── staging/
│   │   ├── __init__.py
│   │   ├── adls_writer.py    # NEW: replaces writer.py
│   │   ├── audit.py          # adapted for Azure SQL
│   │   ├── dw_schema.py      # adapted for Azure SQL (no sqlite branches)
│   │   └── loader.py         # adapted: reads from ADLS instead of local path
│   ├── pipeline/
│   │   ├── __init__.py
│   │   └── runner.py         # adapted: uses adls_writer + azure sql engine
│   └── az_config.py          # NEW: Azure-specific config (Key Vault, App Settings)
├── function_app/
│   ├── __init__.py           # Azure Functions entry point
│   ├── run_pipeline/
│   │   ├── __init__.py       # HTTP-triggered function
│   │   └── function.json
│   ├── host.json
│   └── local.settings.json   # gitignored
├── infra/
│   ├── main.bicep
│   ├── modules/
│   │   ├── adls.bicep
│   │   ├── sql.bicep
│   │   ├── function_app.bicep
│   │   ├── keyvault.bicep
│   │   └── adf.bicep
│   └── parameters.json
├── requirements.txt
├── pyproject.toml
└── .github/
    └── workflows/
        └── deploy.yml
```

Generate a `pyproject.toml` with:
- Python 3.11+
- Package name: `azure-etl`
- Dev dependencies: pytest, ruff, mypy
```

---

## Task 2 — Python Dependencies (`requirements.txt`)

**Prompt:**
```
Write a requirements.txt for the Azure ETL framework. It must include:
- All connectors from single_node_dw (pydantic, polars, sqlalchemy, psycopg, pyodbc,
  oracledb, requests, httpx, paramiko, pymongo, confluent-kafka, pika, nats-py,
  gspread, zeep, webdavclient3, PyYAML)
- Azure SDK packages:
  - azure-storage-file-datalake       (ADLS Gen2)
  - azure-identity                    (DefaultAzureCredential / MSI)
  - azure-keyvault-secrets            (Key Vault integration)
  - azure-functions                   (Azure Function runtime)
  - pyodbc                            (Azure SQL via ODBC Driver 18)
- Remove: psycopg (no PostgreSQL)
- Pin azure-functions>=1.19.0 for Python 3.11 support
```

**Reference block:**
```text
pydantic>=2.8.0
PyYAML>=6.0.2
polars>=1.10.0
sqlalchemy>=2.0.0
pyodbc>=5.1.0
oracledb>=2.2.0
requests>=2.32.0
httpx>=0.27.0
paramiko>=3.4.0
pymongo>=4.8.0
confluent-kafka>=2.6.0
pika>=1.3.2
nats-py>=2.9.0
gspread>=6.1.2
zeep>=4.3.1
webdavclient3>=3.14.6
azure-storage-file-datalake>=12.17.0
azure-identity>=1.19.0
azure-keyvault-secrets>=4.9.0
azure-functions>=1.19.0
```

---

## Task 3 — Azure Configuration Layer (`az_config.py`)

**Prompt:**
```
Create etl/az_config.py that resolves runtime configuration for Azure ETL Functions.
It must:
1. Read connection strings and secrets from Azure App Settings (environment variables)
   using the same _config.py layered loader pattern from single_node_dw.
2. Optionally fetch secrets from Azure Key Vault if the env var AZURE_KEYVAULT_URL
   is set, using DefaultAzureCredential (works with Managed Identity in production,
   and with az login in local dev).
3. Expose three functions:
   - get_adls_config() -> dict  (account_name, container, credential)
   - get_azure_sql_engine() -> sqlalchemy.Engine
   - get_secret(name: str) -> str
4. The ADLS credential must prefer Managed Identity (DefaultAzureCredential).
5. Azure SQL connection string must use:
   mssql+pyodbc://{user}:{password}@{server}/{db}?driver=ODBC+Driver+18+for+SQL+Server
   with Authentication=ActiveDirectoryMsi when running in Azure (no password).
```

**Reference block:**
```python
# etl/az_config.py
import os
from functools import lru_cache

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def _keyvault_client() -> SecretClient | None:
    url = os.getenv("AZURE_KEYVAULT_URL")
    if not url:
        return None
    return SecretClient(vault_url=url, credential=DefaultAzureCredential())


def get_secret(name: str) -> str:
    """Read secret from Key Vault or fall back to environment variable."""
    client = _keyvault_client()
    if client:
        return client.get_secret(name).value
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Secret '{name}' not found in Key Vault or environment")
    return value


def get_adls_config() -> dict:
    return {
        "account_name": os.environ["ADLS_ACCOUNT_NAME"],
        "container": os.environ["ADLS_CONTAINER"],
        "credential": DefaultAzureCredential(),
    }


@lru_cache(maxsize=1)
def get_azure_sql_engine() -> Engine:
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    use_msi  = os.getenv("AZURE_SQL_USE_MSI", "false").lower() == "true"

    if use_msi:
        conn_str = (
            f"mssql+pyodbc://@{server}/{database}"
            "?driver=ODBC+Driver+18+for+SQL+Server"
            "&Authentication=ActiveDirectoryMsi"
        )
    else:
        user     = os.environ["AZURE_SQL_USERNAME"]
        password = os.environ["AZURE_SQL_PASSWORD"]
        conn_str = (
            f"mssql+pyodbc://{user}:{password}@{server}/{database}"
            "?driver=ODBC+Driver+18+for+SQL+Server"
            "&TrustServerCertificate=yes"
        )

    return create_engine(conn_str, pool_pre_ping=True, pool_size=3, max_overflow=5)
```

---

## Task 4 — ADLS Gen2 Writer (`staging/adls_writer.py`)

**Prompt:**
```
Create etl/staging/adls_writer.py that replicates the behavior of writer.py from
single_node_dw but writes Parquet files to Azure Data Lake Storage Gen2 instead of
the local filesystem.

Requirements:
1. Accept the same IngestionResult contract (from connections.sources.data_contract).
2. Write one Parquet file per IngestionResult item, using polars.
3. Use the path convention:
   {container}/{protocol}/{source_name}/YYYY-MM-DD/{safe_source_name}_{timestamp}.parquet
4. Return a list of fully qualified ADLS paths (abfss://container@account.dfs.core.windows.net/...)
   so the loader can reference them.
5. Use azure-storage-file-datalake DataLakeServiceClient with DefaultAzureCredential.
6. Add _ingested_at metadata column exactly as the original writer does.
7. The function signature must be:
   def write_ingestion_result_to_adls(
       result: IngestionResult,
       account_name: str,
       container: str,
       credential,
       source_name: str,
   ) -> list[str]
```

**Reference block:**
```python
# etl/staging/adls_writer.py
import io
from datetime import datetime, timezone as UTC

import polars as pl
from azure.storage.filedatalake import DataLakeServiceClient

from connections._logging import get_logger
from connections.sources.data_contract import IngestionResult

logger = get_logger("staging.adls_writer")


def _payload_to_rows(payload) -> list[dict]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item if isinstance(item, dict) else {"payload": item} for item in payload]
    return [{"payload": payload}]


def _safe_name(value: str) -> str:
    return value.replace("/", "_").replace(".", "_")


def write_ingestion_result_to_adls(
    result: IngestionResult,
    account_name: str,
    container: str,
    credential,
    source_name: str,
) -> list[str]:
    if not result.success or not result.items:
        return []

    service = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential,
    )
    fs_client = service.get_file_system_client(container)
    safe_source = _safe_name(source_name)
    paths: list[str] = []

    for item in result.items:
        rows = _payload_to_rows(item.payload)
        if not rows:
            continue

        now = datetime.now(UTC.utc)
        partition     = now.strftime("%Y-%m-%d")
        file_timestamp = now.strftime("%Y%m%dT%H%M%S%fZ")
        ingested_at   = now.isoformat()

        adls_path = f"{result.protocol}/{source_name}/{partition}/{safe_source}_{file_timestamp}.parquet"

        frame = pl.DataFrame(rows).with_columns(pl.lit(ingested_at).alias("_ingested_at"))

        buf = io.BytesIO()
        frame.write_parquet(buf)
        buf.seek(0)

        file_client = fs_client.get_file_client(adls_path)
        file_client.upload_data(buf.read(), overwrite=True)

        abfss_path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{adls_path}"
        paths.append(abfss_path)

        logger.info(
            "Parquet uploaded to ADLS",
            extra={"protocol": result.protocol, "source": source_name,
                   "rows": frame.height, "path": abfss_path},
        )

    return paths
```

---

## Task 5 — ADLS Loader (`staging/loader.py`)

**Prompt:**
```
Rewrite etl/staging/loader.py for Azure. Instead of reading Parquet from the local
filesystem with polars.read_parquet(path), download the file bytes from ADLS Gen2
using DataLakeFileClient and read them via polars.read_parquet(io.BytesIO(data)).

Keep the same function signature:
  load_parquet_files_to_dw(engine, parquet_paths, table_name, schema) -> int

The parquet_paths list will contain abfss:// URIs returned by adls_writer.py.
Parse account_name and container from the URI to obtain the file client.
Use DefaultAzureCredential passed as a parameter.

The loader must still:
- Call ensure_table_exists (from dw_schema.py) before inserting
- Add _source_file lineage column
- Return total rows inserted
```

**Reference block:**
```python
# etl/staging/loader.py
import io
import re

import polars as pl
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from sqlalchemy import text
from sqlalchemy.engine import Engine

from connections._logging import get_logger
from staging.dw_schema import _quote_identifier, ensure_table_exists

logger = get_logger("staging.loader")

_ABFSS_RE = re.compile(
    r"abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net/(?P<path>.+)"
)


def _download_parquet(uri: str, credential) -> pl.DataFrame:
    m = _ABFSS_RE.match(uri)
    if not m:
        raise ValueError(f"Invalid ADLS URI: {uri}")
    container = m.group("container")
    account   = m.group("account")
    path      = m.group("path")

    service = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=credential,
    )
    file_client = service.get_file_system_client(container).get_file_client(path)
    data = file_client.download_file().readall()
    return pl.read_parquet(io.BytesIO(data))


def load_parquet_files_to_dw(
    engine: Engine,
    parquet_paths: list[str],
    table_name: str,
    schema: str | None = "dbo",
    credential=None,
) -> int:
    if not parquet_paths:
        return 0
    credential = credential or DefaultAzureCredential()
    total_rows = 0

    for uri in parquet_paths:
        frame = _download_parquet(uri, credential)
        if frame.is_empty():
            continue

        ensure_table_exists(engine, table_name, frame, schema=schema)
        source_file = uri.split("/")[-1]
        frame = frame.with_columns(pl.lit(source_file).alias("_source_file"))

        if schema:
            qualified_table = f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"
        else:
            qualified_table = _quote_identifier(table_name)

        columns      = [_quote_identifier(c) for c in frame.columns]
        placeholders = [f":{c}" for c in frame.columns]
        sql = (
            f"INSERT INTO {qualified_table} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        )

        records = frame.to_dicts()
        with engine.connect() as conn:
            conn.execute(text(sql), records)
            conn.commit()

        total_rows += len(records)
        logger.info("Loaded Parquet to Azure SQL",
                    extra={"uri": uri, "table": qualified_table, "rows": len(records)})

    return total_rows
```

---

## Task 6 — Audit Table for Azure SQL (`staging/audit.py`)

**Prompt:**
```
Adapt etl/staging/audit.py from single_node_dw to target Azure SQL Database.
Remove all SQLite branches. Replace BIGSERIAL with IDENTITY(1,1), and
TIMESTAMP WITH TIME ZONE with DATETIMEOFFSET. Keep the same function signatures:
  ensure_audit_table(engine) -> None
  write_audit_record(...) -> None
The table name stays etl_audit_log, default schema is dbo.
```

**Reference block:**
```python
# etl/staging/audit.py  (Azure SQL version)
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.engine import Engine
from connections._logging import get_logger

logger = get_logger("staging.audit")

_DDL = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'etl_audit_log'
)
BEGIN
    CREATE TABLE dbo.etl_audit_log (
        id            BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_id        NVARCHAR(64)  NOT NULL,
        pipeline_name NVARCHAR(256) NOT NULL,
        source_name   NVARCHAR(256) NOT NULL,
        protocol      NVARCHAR(64)  NOT NULL,
        target_table  NVARCHAR(256) NOT NULL,
        status        NVARCHAR(16)  NOT NULL,
        rows_loaded   BIGINT,
        parquet_files INT,
        error_message NVARCHAR(MAX),
        started_at    DATETIMEOFFSET NOT NULL,
        finished_at   DATETIMEOFFSET
    )
END
"""

def ensure_audit_table(engine: Engine) -> None:
    with engine.connect() as conn:
        conn.execute(text(_DDL))
        conn.commit()


def write_audit_record(
    engine: Engine,
    run_id: str,
    pipeline_name: str,
    source_name: str,
    protocol: str,
    target_table: str,
    status: str,
    rows_loaded: int | None,
    parquet_files: int | None,
    started_at: datetime,
    finished_at: datetime | None,
    error_message: str | None = None,
) -> None:
    sql = """
    INSERT INTO dbo.etl_audit_log
        (run_id, pipeline_name, source_name, protocol, target_table,
         status, rows_loaded, parquet_files, started_at, finished_at, error_message)
    VALUES
        (:run_id, :pipeline_name, :source_name, :protocol, :target_table,
         :status, :rows_loaded, :parquet_files, :started_at, :finished_at, :error_message)
    """
    with engine.connect() as conn:
        conn.execute(text(sql), {
            "run_id": run_id, "pipeline_name": pipeline_name,
            "source_name": source_name, "protocol": protocol,
            "target_table": target_table, "status": status,
            "rows_loaded": rows_loaded, "parquet_files": parquet_files,
            "started_at": started_at, "finished_at": finished_at,
            "error_message": error_message,
        })
        conn.commit()
```

---

## Task 7 — DW Schema Helper for Azure SQL (`staging/dw_schema.py`)

**Prompt:**
```
Rewrite etl/staging/dw_schema.py from single_node_dw for Azure SQL Database.
Remove all SQLite dialect branches. Use Azure SQL-specific DDL:
- DATETIMEOFFSET instead of TIMESTAMP WITH TIME ZONE
- GETUTCDATE() instead of now()
- INFORMATION_SCHEMA.TABLES / INFORMATION_SCHEMA.COLUMNS for table introspection
- Square brackets [ ] for identifier quoting instead of double quotes
  (Azure SQL supports both, but square brackets are idiomatic)
- Use IF NOT EXISTS pattern with IF NOT EXISTS check via INFORMATION_SCHEMA
Keep the same public signatures:
  ensure_table_exists(engine, table_name, frame, schema="dbo") -> None
```

**Reference block:**
```python
# etl/staging/dw_schema.py  (Azure SQL version)
import polars as pl
from sqlalchemy import text
from sqlalchemy.engine import Engine
from connections._logging import get_logger

logger = get_logger("staging.dw_schema")


def _quote_identifier(value: str) -> str:
    return f"[{value.replace(']', ']]')}]"


def _polars_to_azuresql(dtype: pl.DataType) -> str:
    name = str(dtype)
    if name in {"Utf8", "String"}:        return "NVARCHAR(MAX)"
    if name in {"Int32", "Int64"}:        return "BIGINT"
    if name in {"Float32", "Float64"}:    return "FLOAT"
    if name == "Boolean":                 return "BIT"
    if name == "Date":                    return "DATE"
    if name.startswith("Datetime"):       return "DATETIMEOFFSET"
    return "NVARCHAR(MAX)"


def _table_exists(engine: Engine, schema: str, table_name: str) -> bool:
    sql = text("""
        SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
    """)
    with engine.connect() as conn:
        return bool(conn.execute(sql, {"schema": schema, "table": table_name}).scalar())


def _existing_columns(engine: Engine, schema: str, table_name: str) -> set[str]:
    sql = text("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table
    """)
    with engine.connect() as conn:
        return {row[0] for row in conn.execute(sql, {"schema": schema, "table": table_name})}


def ensure_table_exists(
    engine: Engine,
    table_name: str,
    frame: pl.DataFrame,
    schema: str = "dbo",
) -> None:
    qualified = f"{_quote_identifier(schema)}.{_quote_identifier(table_name)}"

    col_defs = [
        f"{_quote_identifier(c)} {_polars_to_azuresql(frame.schema[c])}"
        for c in frame.columns
    ]
    col_defs.append("[_loaded_at] DATETIMEOFFSET DEFAULT GETUTCDATE()")
    col_defs.append("[_source_file] NVARCHAR(512)")

    ddl = f"""
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
    )
    BEGIN
        CREATE TABLE {qualified} ({', '.join(col_defs)})
    END
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()

    existing = _existing_columns(engine, schema, table_name)
    new_cols = [
        (c, _polars_to_azuresql(frame.schema[c]))
        for c in frame.columns if c not in existing
    ]
    if not new_cols:
        return
    with engine.connect() as conn:
        for col, col_type in new_cols:
            conn.execute(text(
                f"ALTER TABLE {qualified} ADD {_quote_identifier(col)} {col_type}"
            ))
        conn.commit()
```

---

## Task 8 — Pipeline Runner (`pipeline/runner.py`)

**Prompt:**
```
Rewrite etl/pipeline/runner.py for Azure. Replace:
- write_ingestion_result_to_parquet  →  write_ingestion_result_to_adls
- load_parquet_files_to_dw           →  load_parquet_files_to_dw (azure loader)
- dw_engine parameter stays the same but is now Azure SQL

The runner must accept adls config (account_name, container, credential) as a
separate argument group. Everything else (run_id, audit writes, error handling)
stays identical to the original runner.

Function signature:
  run_pipeline(
      connector_config: dict,
      query: str,
      source_name: str,
      target_table: str,
      adls_account: str,
      adls_container: str,
      adls_credential,
      dw_engine: Engine,
      schema: str = "dbo",
      pipeline_name: str = "default",
  ) -> dict
```

**Reference block:**
```python
# etl/pipeline/runner.py  (Azure version)
import uuid
from datetime import datetime, timezone as UTC

from sqlalchemy.engine import Engine

from connections._logging import get_logger
from connections.sources.factory import create_connector
from staging import (
    ensure_audit_table,
    write_audit_record,
)
from staging.adls_writer import write_ingestion_result_to_adls
from staging.loader import load_parquet_files_to_dw

logger = get_logger("pipeline.runner")


def run_pipeline(
    connector_config: dict,
    query: str,
    source_name: str,
    target_table: str,
    adls_account: str,
    adls_container: str,
    adls_credential,
    dw_engine: Engine,
    schema: str = "dbo",
    pipeline_name: str = "default",
) -> dict:
    run_id     = str(uuid.uuid4())
    started_at = datetime.now(UTC.utc)
    protocol   = connector_config.get("protocol", "unknown")

    ensure_audit_table(dw_engine)

    try:
        connector = create_connector(connector_config)
        connector.connect()
        try:
            result = connector.fetch_data(query)
        finally:
            connector.close()

        if not result.success:
            error_msg = f"Ingestion failed: {result.metadata.get('error')}"
            write_audit_record(dw_engine, run_id, pipeline_name, source_name, protocol,
                               target_table, "failure", 0, 0, started_at,
                               datetime.now(UTC.utc), error_msg)
            return {"run_id": run_id, "status": "failure", "error": error_msg}

        parquet_paths = write_ingestion_result_to_adls(
            result=result,
            account_name=adls_account,
            container=adls_container,
            credential=adls_credential,
            source_name=source_name,
        )

        rows_loaded = load_parquet_files_to_dw(
            engine=dw_engine,
            parquet_paths=parquet_paths,
            table_name=target_table,
            schema=schema,
            credential=adls_credential,
        )

        finished_at = datetime.now(UTC.utc)
        write_audit_record(dw_engine, run_id, pipeline_name, source_name, protocol,
                           target_table, "success", rows_loaded, len(parquet_paths),
                           started_at, finished_at)

        return {
            "run_id": run_id,
            "status": "success",
            "rows_loaded": rows_loaded,
            "parquet_files": len(parquet_paths),
            "duration_seconds": (finished_at - started_at).total_seconds(),
        }

    except Exception as e:
        logger.exception("Pipeline execution failed", extra={"run_id": run_id})
        write_audit_record(dw_engine, run_id, pipeline_name, source_name, protocol,
                           target_table, "failure", 0, 0, started_at,
                           datetime.now(UTC.utc), str(e))
        raise
```

---

## Task 9 — Azure Function App Entry Point

**Prompt:**
```
Create an Azure Function App (v2 programming model, Python 3.11) with one
HTTP-triggered function called run_pipeline.

The function must:
1. Accept a POST body with JSON:
   {
     "connector_config": { "protocol": "...", ... },
     "query": "...",
     "source_name": "...",
     "target_table": "...",
     "pipeline_name": "...",
     "schema": "dbo"
   }
2. Load ADLS and Azure SQL config from App Settings via az_config.py.
3. Call pipeline.runner.run_pipeline() with the parsed body.
4. Return 200 with the result dict on success, or 500 with error message on failure.
5. Use azure-functions v2 decorator style (not function.json).
6. Add a /health GET endpoint that returns {"status": "ok"}.
```

**Reference block:**
```python
# function_app/__init__.py  (Azure Functions v2 programming model)
import json
import logging

import azure.functions as func

from az_config import get_adls_config, get_azure_sql_engine
from pipeline.runner import run_pipeline

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
logger = logging.getLogger("function_app")


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def health(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(json.dumps({"status": "ok"}), mimetype="application/json")


@app.route(route="run_pipeline", methods=["POST"])
def run_pipeline_func(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body", status_code=400)

    required = ["connector_config", "query", "source_name", "target_table"]
    missing  = [k for k in required if k not in body]
    if missing:
        return func.HttpResponse(
            json.dumps({"error": f"Missing fields: {missing}"}),
            status_code=400, mimetype="application/json"
        )

    try:
        adls  = get_adls_config()
        engine = get_azure_sql_engine()

        result = run_pipeline(
            connector_config=body["connector_config"],
            query=body["query"],
            source_name=body["source_name"],
            target_table=body["target_table"],
            adls_account=adls["account_name"],
            adls_container=adls["container"],
            adls_credential=adls["credential"],
            dw_engine=engine,
            schema=body.get("schema", "dbo"),
            pipeline_name=body.get("pipeline_name", "default"),
        )
        return func.HttpResponse(json.dumps(result), mimetype="application/json")

    except Exception as exc:
        logger.exception("Pipeline failed")
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            status_code=500, mimetype="application/json"
        )
```

**`host.json`:**
```json
{
  "version": "2.0",
  "logging": {
    "applicationInsights": { "samplingSettings": { "isEnabled": true } }
  },
  "functionTimeout": "00:10:00",
  "extensions": { "http": { "routePrefix": "api" } }
}
```

**`local.settings.json` (gitignored):**
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "ADLS_ACCOUNT_NAME": "myadlsaccount",
    "ADLS_CONTAINER": "staging",
    "AZURE_SQL_SERVER": "myserver.database.windows.net",
    "AZURE_SQL_DATABASE": "mydb",
    "AZURE_SQL_USERNAME": "etluser",
    "AZURE_SQL_PASSWORD": "...",
    "AZURE_SQL_USE_MSI": "false",
    "AZURE_KEYVAULT_URL": ""
  }
}
```

---

## Task 10 — Azure Infrastructure (Bicep)

**Prompt:**
```
Create Azure Bicep templates to provision all infrastructure for the ETL framework.
Use a modular structure with a main.bicep that calls child modules.

Resources to provision:
1. Resource Group (or assume existing)
2. Azure Data Lake Storage Gen2
   - Hierarchical namespace enabled
   - Container: "staging"
   - LRS redundancy for cost
3. Azure SQL Database
   - Server + Database (S1 tier or Basic for dev)
   - SQL auth + AAD admin
   - Firewall: allow Azure services
4. Azure Function App
   - Consumption plan (Y1) for cost, or Premium EP1 for production
   - Python 3.11 runtime
   - App Settings wired to ADLS and SQL
   - Managed Identity enabled (system-assigned)
5. Azure Key Vault
   - RBAC mode
   - Function App MSI gets "Key Vault Secrets User" role
6. Azure Data Factory
   - Linked Service: Azure Function App
   - One pipeline template with a single Azure Function Activity
7. Role assignments:
   - Function App MSI → "Storage Blob Data Contributor" on ADLS
   - Function App MSI → Azure SQL contained user (handled via post-deploy script)
```

**Reference block — `infra/main.bicep`:**
```bicep
targetScope = 'resourceGroup'

param location string = resourceGroup().location
param prefix string = 'etl'
param sqlAdminLogin string
@secure()
param sqlAdminPassword string

module adls 'modules/adls.bicep' = {
  name: 'adls'
  params: { location: location, prefix: prefix }
}

module sql 'modules/sql.bicep' = {
  name: 'sql'
  params: {
    location: location
    prefix: prefix
    adminLogin: sqlAdminLogin
    adminPassword: sqlAdminPassword
  }
}

module kv 'modules/keyvault.bicep' = {
  name: 'keyvault'
  params: { location: location, prefix: prefix }
}

module func 'modules/function_app.bicep' = {
  name: 'functionapp'
  params: {
    location: location
    prefix: prefix
    adlsAccountName: adls.outputs.accountName
    adlsContainer: 'staging'
    sqlServer: sql.outputs.serverFqdn
    sqlDatabase: sql.outputs.databaseName
    keyVaultUri: kv.outputs.uri
  }
}

module adf 'modules/adf.bicep' = {
  name: 'adf'
  params: {
    location: location
    prefix: prefix
    functionAppUrl: func.outputs.defaultHostName
  }
}

// Role: Function MSI → Storage Blob Data Contributor on ADLS
resource adlsRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(adls.outputs.resourceId, func.outputs.principalId, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: resourceGroup()
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: func.outputs.principalId
    principalType: 'ServicePrincipal'
  }
}
```

**Reference block — `infra/modules/adls.bicep`:**
```bicep
param location string
param prefix string

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: '${prefix}adls${uniqueString(resourceGroup().id)}'
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  name: '${storage.name}/default/staging'
  properties: { publicAccess: 'None' }
}

output accountName string = storage.name
output resourceId string = storage.id
```

---

## Task 11 — Azure Data Factory Pipeline

**Prompt:**
```
Create an Azure Data Factory pipeline (JSON definition) that calls the Azure Function
run_pipeline endpoint for each configured ETL job.

The pipeline must:
1. Use a ForEach activity that iterates over a pipeline parameter "etl_jobs" (array).
2. Each element of etl_jobs is a JSON object matching the run_pipeline HTTP body schema.
3. Inside the ForEach, use an Azure Function Activity that:
   - Calls the Function App Linked Service
   - Uses POST method
   - Passes the current item as the body
4. Add a timeout of 10 minutes per function call.
5. Add an error path that writes failed job names to a Set Variable activity.

Also write the ADF Linked Service definition for the Azure Function App using
Managed Identity authentication.
```

**Reference block — ADF pipeline JSON:**
```json
{
  "name": "etl_orchestration",
  "properties": {
    "parameters": {
      "etl_jobs": { "type": "array" }
    },
    "activities": [
      {
        "name": "ForEachJob",
        "type": "ForEach",
        "typeProperties": {
          "isSequential": false,
          "batchCount": 4,
          "items": { "value": "@pipeline().parameters.etl_jobs", "type": "Expression" },
          "activities": [
            {
              "name": "RunETLFunction",
              "type": "AzureFunctionActivity",
              "typeProperties": {
                "functionName": "run_pipeline",
                "method": "POST",
                "body": { "value": "@item()", "type": "Expression" },
                "headers": { "Content-Type": "application/json" }
              },
              "linkedServiceName": { "referenceName": "AzureFunctionLinkedService", "type": "LinkedServiceReference" },
              "timeout": "0.00:10:00",
              "policy": { "retry": 1, "retryIntervalInSeconds": 30 }
            }
          ]
        }
      }
    ]
  }
}
```

**Reference block — ADF Linked Service (MSI):**
```json
{
  "name": "AzureFunctionLinkedService",
  "properties": {
    "type": "AzureFunction",
    "typeProperties": {
      "functionAppUrl": "https://<function-app-name>.azurewebsites.net",
      "authentication": "MSI",
      "resourceId": "https://management.azure.com/"
    }
  }
}
```

---

## Task 12 — Connector Reuse Guide

**Prompt:**
```
Copy the following modules from single_node_dw/etl to azure_etl/etl without changes.
They are infrastructure-agnostic and work identically in Azure Functions:

  connections/_config.py
  connections/_engine_cache.py
  connections/_logging.py
  connections/_session_cache.py
  connections/sources/base_connector.py
  connections/sources/data_contract.py
  connections/sources/factory.py
  connections/sources/sql/config.py
  connections/sources/sql/incremental.py
  connections/sources/sql/mssql.py        ← primary DB connector for Azure SQL sources
  connections/sources/sql/postgres.py
  connections/sources/sql/oracle.py
  connections/sources/http/config.py
  connections/sources/http/connector.py
  connections/sources/http/rest.py
  connections/sources/http/soap_connector.py
  connections/sources/ftp/config.py
  connections/sources/ftp/connector.py
  connections/sources/ftp/webdav_connector.py
  connections/sources/nosql/mongodb/config.py
  connections/sources/nosql/mongodb/connector.py
  connections/sources/saas/gsheets/config.py
  connections/sources/saas/gsheets/connector.py
  connections/sources/streams/amqp.py
  connections/sources/streams/kafka.py
  connections/sources/streams/nats.py
  connections/sources/ssh/config.py
  connections/sources/ssh/connector.py

Update all relative imports from staging.writer to staging.adls_writer after copying.
The factory.py and base_connector.py need zero changes.
The dw_destination.py (PostgreSQL-specific) should NOT be copied — Azure SQL
connection is handled by az_config.get_azure_sql_engine().
```

---

## Task 13 — CI/CD with GitHub Actions

**Prompt:**
```
Create a GitHub Actions workflow (.github/workflows/deploy.yml) that:
1. Triggers on push to main branch.
2. Runs Python tests (pytest etl/tests/).
3. Runs ruff linter.
4. On success, deploys the Azure Function App using:
   azure/functions-action@v1
   with publish-profile from GitHub Secret AZURE_FUNCTIONAPP_PUBLISH_PROFILE.
5. Uses Python 3.11.
6. Caches pip dependencies.
7. Sets FUNCTIONS_WORKER_RUNTIME to python.
```

**Reference block:**
```yaml
# .github/workflows/deploy.yml
name: Deploy ETL Function App

on:
  push:
    branches: [main]

env:
  PYTHON_VERSION: "3.11"
  AZURE_FUNCTIONAPP_NAME: "etl-function-app"
  AZURE_FUNCTIONAPP_PACKAGE_PATH: "."

jobs:
  test-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: pip

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Lint
        run: ruff check etl/

      - name: Test
        run: pytest etl/tests/ -v
        env:
          ADLS_ACCOUNT_NAME: "mock"
          ADLS_CONTAINER: "staging"
          AZURE_SQL_SERVER: "mock"
          AZURE_SQL_DATABASE: "mock"
          AZURE_SQL_USERNAME: "mock"
          AZURE_SQL_PASSWORD: "mock"

      - name: Deploy to Azure Functions
        uses: azure/functions-action@v1
        with:
          app-name: ${{ env.AZURE_FUNCTIONAPP_NAME }}
          package: ${{ env.AZURE_FUNCTIONAPP_PACKAGE_PATH }}
          publish-profile: ${{ secrets.AZURE_FUNCTIONAPP_PUBLISH_PROFILE }}
          scm-do-build-during-deployment: true
          enable-oryx-build: true
```

---

## Task 14 — Local Development Setup

**Prompt:**
```
Write a local development guide and a Makefile for the Azure ETL framework.
The developer workflow must:
1. Use Azure Functions Core Tools (func start) to run functions locally.
2. Use Azurite (Docker) as the local ADLS emulator.
3. Use a local SQL Server (Docker, mcr.microsoft.com/mssql/server:2022-latest)
   as the Azure SQL substitute.
4. Use a Makefile with targets:
   - make dev      → start Azurite + SQL Server containers
   - make func     → run func start
   - make test     → pytest
   - make lint     → ruff check + ruff format --check
   - make infra    → az deployment group create with Bicep
5. Document environment variables required in local.settings.json.
```

**Reference block — Makefile:**
```makefile
.PHONY: dev func test lint infra

RESOURCE_GROUP ?= rg-etl-dev
LOCATION       ?= eastus

dev:
	docker run -d --name azurite -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite-blob --loose
	docker run -d --name sqlserver -e ACCEPT_EULA=Y -e SA_PASSWORD=YourStrong!Passw0rd \
		-p 1433:1433 mcr.microsoft.com/mssql/server:2022-latest

func:
	cd function_app && func start --python

test:
	pytest etl/tests/ -v

lint:
	ruff check etl/
	ruff format --check etl/

infra:
	az deployment group create \
		--resource-group $(RESOURCE_GROUP) \
		--template-file infra/main.bicep \
		--parameters @infra/parameters.json
```

---

## Build Order Summary

Execute these tasks in sequence to build the framework:

| # | Task | Depends On |
|---|------|-----------|
| 1 | Repository structure + pyproject.toml | — |
| 2 | requirements.txt | 1 |
| 3 | `az_config.py` (Key Vault + App Settings) | 2 |
| 4 | Copy connectors from single_node_dw | 1 |
| 5 | `staging/adls_writer.py` | 2, 3 |
| 6 | `staging/audit.py` (Azure SQL DDL) | 2 |
| 7 | `staging/dw_schema.py` (Azure SQL) | 2 |
| 8 | `staging/loader.py` (ADLS download → Azure SQL) | 5, 6, 7 |
| 9 | `pipeline/runner.py` | 4, 5, 8 |
| 10 | Function App entry point | 3, 9 |
| 11 | Bicep infra templates | — |
| 12 | ADF pipeline JSON | 11 |
| 13 | GitHub Actions workflow | 1–10 |
| 14 | Local dev setup (Makefile + Azurite) | 1–10 |
