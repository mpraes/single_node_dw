# 🎭 Mage.ai Orchestration Integration

Your ETL framework now includes **Mage.ai orchestration** for professional pipeline scheduling, monitoring, and workflow management.

## 🚀 Quick Start

### 1. Start Infrastructure
```bash
make infra-up
```

### 2. Access Mage.ai
Open [http://localhost:6789](http://localhost:6789) in your browser

### 3. Setup Your First Pipeline
1. Create new pipeline in Mage UI
2. Copy blocks from `mage_blocks/` to your Mage project
3. Use templates from `mage_templates/pipelines/`

## 📋 What's Included

### Custom Mage Blocks
- **ETL Runner** (`mage_blocks/custom/etl_runner.py`) - Core integration functions
- **Source Extractor** (`mage_blocks/data_loaders/`) - Extract using ETL connectors  
- **DW Loader** (`mage_blocks/data_exporters/`) - Load to DW with audit
- **Full Pipeline** (`mage_blocks/data_exporters/`) - Complete ETL via CLI

### Pipeline Templates
- **Simple ETL** (`mage_templates/pipelines/simple_etl_pipeline.py`) - Extract → Transform → Load
- **Full Pipeline** (`mage_templates/pipelines/full_pipeline_block.py`) - Complete CLI execution

### Configuration Examples
- **PostgreSQL** (`mage_templates/configs/postgres_source.json`)
- **REST API** (`mage_templates/configs/rest_api_source.json`)
- **MongoDB** (`mage_templates/configs/mongodb_source.json`)

## 🔄 Integration Patterns

### Pattern 1: Full Pipeline (Recommended)
```python
# Execute your existing ETL pipelines through Mage
# Benefits: Simple, uses existing CLI, full audit
execute_etl_pipeline(
    config_path="connector.json",
    query="SELECT * FROM users", 
    source_name="postgres_prod",
    target_table="stg_users"
)
```

### Pattern 2: Granular Blocks  
```python
# Extract → Transform → Load with Mage processing
# Benefits: Data transformation, quality checks
df = extract_from_source(connector_config)
df_clean = transform_data(df) 
load_to_dw(df_clean, "stg_users")
```

## 📅 Scheduling Examples

```bash
# Daily at 2 AM
0 2 * * *

# Every 4 hours
0 */4 * * *

# Business days at 9 AM  
0 9 * * 1-5
```

## 🔍 Monitoring

- **Mage UI**: Real-time pipeline status and logs
- **Audit Tables**: Your existing audit system works seamlessly
```sql
SELECT pipeline_name, status, rows_loaded, duration
FROM etl_audit_log 
WHERE pipeline_name LIKE 'mage_%'
ORDER BY started_at DESC;
```

## 📖 Complete Documentation

See **[docs/guides/mage-orchestration.md](docs/guides/mage-orchestration.md)** for:
- Detailed setup instructions
- Configuration management
- Best practices
- Troubleshooting guide
- Advanced features

## 🎯 Benefits

✅ **Professional UI** - Visual pipeline builder and monitoring  
✅ **Scheduling** - Cron-based and event-driven triggers  
✅ **Dependencies** - Pipeline orchestration with dependencies  
✅ **Monitoring** - Real-time status, logs, and alerting  
✅ **Existing Code** - Uses your current ETL framework  
✅ **Full Audit** - Maintains your audit trail  

Your ETL framework is now production-ready with enterprise-grade orchestration! 🎉