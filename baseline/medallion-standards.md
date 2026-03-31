# Medallion Standards

Shared coding standards and runtime assumptions applied across all scenarios.

## Spark Runtime

- **Runtime:** Fabric Spark Runtime 1.3 (Spark 3.5, Delta Lake 3.x, Python 3.11)
- **Default pool:** Starter pool for ≤10 min tasks; Custom pool (4 Medium nodes) for heavy jobs
- **Library management:** Fabric Environment attached to the workspace

## Standard Notebook Header

Every notebook begins with this cell:

```python
# ── Configuration ──────────────────────────────────────────────────────────────
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

LAKEHOUSE = "lh_advanced_scenarios"
BRONZE    = f"{LAKEHOUSE}.bronze"
SILVER    = f"{LAKEHOUSE}.silver"
GOLD      = f"{LAKEHOUSE}.gold"
```

## Schema Evolution Policy

| Scenario | `mergeSchema` | `overwriteSchema` |
|---|---|---|
| Bronze append | `true` | `false` |
| Silver MERGE | `false` | `false` |
| Gold full refresh | `false` | `true` (first load only) |

## Watermarking Convention

Watermarks are stored in a control table `lh_advanced_scenarios.bronze.pipeline_watermark`:

```sql
CREATE TABLE IF NOT EXISTS bronze.pipeline_watermark (
    pipeline_name  STRING  NOT NULL,
    entity_name    STRING  NOT NULL,
    watermark_col  STRING  NOT NULL,
    last_value     TIMESTAMP,
    updated_at     TIMESTAMP
)
USING DELTA
```

All incremental pipelines read the last watermark from this table before querying the source
and update it atomically after a successful write.

## File Format Standards

| Layer | Format | Compression | Max file size target |
|---|---|---|---|
| Landing (Files/) | Parquet or CSV | gzip / snappy | 256 MB |
| Bronze | Delta (Parquet) | snappy | 128 MB (after OPTIMIZE) |
| Silver | Delta (Parquet) | snappy | 128 MB |
| Gold | Delta (Parquet) | snappy | 64 MB |

Run `OPTIMIZE <table> ZORDER BY (<cols>)` and `VACUUM <table> RETAIN 168 HOURS` weekly
via a scheduled maintenance notebook.

## Error Handling Standard

```python
from datetime import datetime, timezone

def log_error(pipeline: str, entity: str, error: str) -> None:
    """Write a structured error record to the pipeline_errors control table."""
    spark.createDataFrame([{
        "pipeline_name": pipeline,
        "entity_name":   entity,
        "error_message": error,
        "logged_at":     datetime.now(timezone.utc),
    }]).write.format("delta").mode("append").saveAsTable("bronze.pipeline_errors")
```
