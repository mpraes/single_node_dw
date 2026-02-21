# Mage.ai Orchestration Guide

This guide explains how to use Mage.ai as an orchestration layer for your ETL pipelines, providing scheduling, monitoring, and workflow management capabilities.

## Overview

The integration combines your existing ETL framework with Mage.ai's orchestration capabilities:

- **Your ETL Framework**: Handles connectors, data extraction, staging, and loading
- **Mage.ai**: Provides scheduling, workflow management, monitoring UI, and dependency management
- **Shared Infrastructure**: PostgreSQL DW, Docker environment, configuration management

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Mage.ai UI    │    │  Custom Blocks   │    │ ETL Framework   │
│   (Port 6789)   │◄───┤  Integration     ├───►│   CLI & API     │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
        │                        │                       │
        └────────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │   PostgreSQL DW        │
                    │   Audit & Data Tables  │
                    └─────────────────────────┘
```

## Quick Start

### 1. Start the Infrastructure

```bash
# Start PostgreSQL and Mage.ai
make infra-up

# Check services are running
make infra-status
```

### 2. Access Mage.ai

Open your browser to [http://localhost:6789](http://localhost:6789)

### 3. Setup Your First Pipeline

1. **Create New Pipeline** in Mage UI
2. **Copy Custom Blocks** from `mage_blocks/` to your Mage project
3. **Use Templates** from `mage_templates/pipelines/`

## Integration Patterns

### Pattern 1: Full Pipeline Block (Recommended for Simple ETL)

Use this when you want to run your existing ETL pipelines through Mage with minimal changes.

```python
# Use: mage_templates/pipelines/full_pipeline_block.py
# Benefits: Simple, uses existing CLI, full audit trail
# Best for: Direct source-to-DW pipelines
```

**Example Pipeline:**
1. **Trigger Block**: Starts the pipeline
2. **ETL Pipeline Block**: Executes your full ETL CLI command
3. **Result**: Data loaded to DW with full auditing

### Pattern 2: Granular ETL Blocks (Advanced Processing)

Use this when you need data transformation within Mage or complex workflow logic.

```python
# Use: mage_templates/pipelines/simple_etl_pipeline.py  
# Benefits: Full Mage capabilities, intermediate processing
# Best for: Complex transformations, data quality checks
```

**Example Pipeline:**
1. **Data Loader**: Extract using ETL connectors → DataFrame
2. **Transformer**: Clean/transform data with pandas
3. **Data Exporter**: Load to DW using ETL framework

### Pattern 3: Hybrid Workflows

Combine both patterns for complex scenarios:
- Use ETL blocks for extraction and staging
- Use Mage blocks for complex transformations
- Use full pipeline blocks for final loading

## Configuration Management

### Environment Variables

Your ETL framework environment variables are automatically available in Mage containers:

```bash
# In .env file (automatically mounted to Mage)
DW_HOST=postgres
DW_PORT=5432
DW_DATABASE=dw_db
DW_USERNAME=dw_user
DW_PASSWORD=dw_password
```

### Connector Configurations

Store connector configs in your Mage project or use the templates:

```json
// Example: REST API connector
{
  "protocol": "http",
  "base_url": "https://api.example.com",
  "timeout_seconds": 30
}
```

```json
// Example: Database connector  
{
  "protocol": "postgres",
  "host": "source-db.local",
  "port": 5432,
  "database": "source_db",
  "username": "user",
  "password": "password"
}
```

## Scheduling & Triggers

### Cron Scheduling

In Mage UI:
1. Go to **Pipeline Settings** → **Triggers**
2. Add **Schedule Trigger**
3. Set cron expression:

```bash
# Daily at 2 AM
0 2 * * *

# Every hour
0 * * * *

# Business days at 9 AM
0 9 * * 1-5
```

### Event Triggers

- **API Triggers**: HTTP endpoints to start pipelines
- **File Triggers**: Start when files appear
- **Webhook Triggers**: Integration with external systems

### Dependencies

Create pipeline dependencies in Mage:
1. **Sequential**: Pipeline A → Pipeline B → Pipeline C
2. **Parallel**: Multiple pipelines run simultaneously
3. **Conditional**: Run based on previous pipeline results

## Monitoring & Observability

### Mage.ai UI Dashboard

- **Pipeline Status**: Real-time execution status
- **Run History**: Complete execution history
- **Logs**: Detailed logs for each block
- **Metrics**: Runtime, success rates, data volumes

### ETL Framework Audit

Your existing audit system works seamlessly:

```sql
-- Check recent pipeline runs
SELECT 
    pipeline_name,
    source_name,
    target_table,
    status,
    rows_loaded,
    started_at,
    finished_at - started_at as duration
FROM etl_audit_log 
WHERE pipeline_name LIKE 'mage_%'
ORDER BY started_at DESC;
```

### Alerting

Set up alerts in Mage UI:
- **Failure Alerts**: Email/Slack when pipelines fail
- **Success Notifications**: Confirm successful runs
- **Performance Alerts**: Long-running pipeline warnings

## Best Practices

### 1. Pipeline Design

```python
# ✅ Good: Clear, descriptive pipeline names
pipeline_name = "daily_sales_extract_to_dw"

# ❌ Bad: Generic names
pipeline_name = "pipeline1"
```

### 2. Error Handling

```python
# ✅ Good: Proper error handling with context
try:
    result = execute_etl_pipeline(...)
    if not result["success"]:
        raise Exception(f"Pipeline failed: {result.get('error')}")
except Exception as e:
    # Log error details
    print(f"❌ Pipeline {pipeline_name} failed: {str(e)}")
    # Re-raise for Mage to handle
    raise
```

### 3. Resource Management

```python
# ✅ Good: Clean up temporary files
try:
    temp_file = create_temp_config(config)
    result = execute_pipeline(temp_file)
finally:
    cleanup_temp_file(temp_file)
```

### 4. Configuration Security

```bash
# ✅ Good: Use environment variables for secrets
DW_PASSWORD=${SECRET_DW_PASSWORD}

# ❌ Bad: Hardcode passwords
DW_PASSWORD=mypassword123
```

## Troubleshooting

### Common Issues

**1. Import Errors in Mage Blocks**
```python
# Add ETL framework to path
import sys
sys.path.append('/app/etl')
```

**2. Permission Issues with Lake Directory**
```bash
# Ensure lake directory is writable
chmod 755 ./lake
```

**3. Connection Timeouts**
```python
# Increase timeout in connector config
{
  "protocol": "http",
  "timeout_seconds": 60  # Increased from 30
}
```

**4. Memory Issues with Large Datasets**
```python
# Use ETL framework's streaming capabilities
# Avoid loading entire dataset into memory at once
```

### Debugging Steps

1. **Check Mage Logs**: View detailed logs in Mage UI
2. **Test Connections**: Use connection test blocks first
3. **Verify Mounts**: Ensure ETL code is properly mounted
4. **Check Environment**: Verify environment variables are set

## Migration from Manual CLI

### Step 1: Identify Current Pipelines

```bash
# List your current CLI pipelines
find . -name "*.sh" -o -name "pipeline_*.py"
```

### Step 2: Convert to Mage

For each pipeline:
1. Extract connector configuration to JSON
2. Create Mage pipeline using templates
3. Test with small data first
4. Schedule in Mage UI

### Step 3: Gradually Transition

- Start with non-critical pipelines
- Run both systems in parallel initially  
- Monitor performance and reliability
- Complete migration when confident

## Advanced Features

### Custom Block Development

Create your own Mage blocks for specific needs:

```python
@data_loader
def custom_source_loader(*args, **kwargs):
    # Your custom extraction logic
    return dataframe

@transformer  
def custom_data_processor(df, *args, **kwargs):
    # Your custom transformation logic
    return processed_df
```

### Dynamic Pipeline Generation

Use Mage's Python API to create pipelines programmatically:

```python
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler

# Create pipelines based on configuration
scheduler = PipelineScheduler()
scheduler.create_pipeline_from_template(template_name, config)
```

### Integration with External Systems

- **Data Quality**: Integrate with Great Expectations
- **Metadata**: Connect to data catalog systems  
- **Notifications**: Slack, email, PagerDuty integration
- **CI/CD**: Git-based pipeline deployment

## Support

- **Mage.ai Documentation**: [https://docs.mage.ai](https://docs.mage.ai)
- **ETL Framework**: See existing documentation in `docs/`
- **Community**: Mage.ai Slack community

## Next Steps

1. **Start Simple**: Begin with full pipeline blocks
2. **Add Scheduling**: Set up your first scheduled pipeline
3. **Monitor Results**: Use both Mage UI and audit tables
4. **Expand Gradually**: Add more complex workflows
5. **Optimize**: Fine-tune performance and reliability