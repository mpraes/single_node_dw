# ETL Pipeline Runner

The pipeline runner is the core orchestration layer of the Single Node DW. It manages the full lifecycle of data: extraction, staging as Parquet, and loading into the PostgreSQL Data Warehouse with full auditing.

## Infrastructure Setup

Before running pipelines, ensure your PostgreSQL Data Warehouse is running. You can use the provided Docker Compose configuration:

```bash
make infra-up
```

This starts a PostgreSQL instance accessible at `localhost:5432`.

## Configuration

Configure your connections in a `.env` file (see `.env.example` for reference). At a minimum, you need the Data Warehouse credentials:

```bash
DW_HOST=localhost
DW_PORT=5432
DW_DATABASE=dw
DW_USERNAME=postgres
DW_PASSWORD=postgres
```

## Running Pipelines via CLI

You can run pipelines using the `etl.cli` module. The CLI expects a connector configuration file (JSON or YAML).

### Example: Syncing from a REST API

Create a `connector.json`:

```json
{
  "protocol": "http",
  "base_url": "https://jsonplaceholder.typicode.com"
}
```

Run the pipeline:

```bash
python -m etl.cli run 
  --config connector.json 
  --query "/users" 
  --source api_users 
  --table stg_users 
  --lake ./lake
```

### Parameters

- `--config`: Path to the source connector configuration.
- `--query`: The resource to fetch (SQL query for databases, relative path for HTTP).
- `--source`: A logical name for the source system.
- `--table`: The target table name in the DW.
- `--lake`: Local directory where Parquet files will be staged.
- `--schema`: (Optional) Target schema in the DW (default: `public`).
- `--pipeline`: (Optional) Logical name for this pipeline run (default: `default`).

## Testing Connections

Verify your setup with the `test-connection` command:

```bash
# Test DW connection
python -m etl.cli test-connection --source dw

# Test a source connection
python -m etl.cli test-connection --config connector.json
```

## Audit Log

Every pipeline run is recorded in the `etl_audit_log` table. You can query it to check for status and performance:

```sql
SELECT 
    pipeline_name, 
    source_name, 
    status, 
    rows_loaded, 
    started_at, 
    finished_at - started_at as duration
FROM etl_audit_log
ORDER BY started_at DESC;
```

## Error Handling

If a pipeline fails:
1. The error is logged to the console (stderr).
2. A record is written to `etl_audit_log` with `status = 'failure'` and the `error_message`.
3. The process exits with code `1`.
