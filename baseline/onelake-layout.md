# OneLake Layout

All data lives under the **single Lakehouse** `lh_advanced_scenarios`. The folder hierarchy
follows the medallion pattern and is enforced by pipeline and notebook conventions.

## Folder Structure

```
lh_advanced_scenarios/
├── Files/
│   ├── landing/                   # Raw drops from external sources (pre-Bronze)
│   │   └── <source>/<YYYY-MM-DD>/
│   └── quarantine/                # Records rejected by quality checks
│       └── <entity>/<YYYY-MM-DD>/
└── Tables/
    ├── bronze/                    # Raw Delta tables, schema-on-read, append-only
    │   └── <entity>              #   e.g. bronze.orders_raw
    ├── silver/                    # Cleansed, deduplicated, typed Delta tables
    │   └── <entity>              #   e.g. silver.orders
    └── gold/                      # Aggregated, conformed, business-ready tables
        ├── dim_<entity>          #   e.g. gold.dim_customer
        └── fact_<entity>         #   e.g. gold.fact_sales
```

## Layer Contracts

### Bronze

| Property | Value |
|---|---|
| Write mode | Append-only (no DELETE, no UPDATE) |
| Schema enforcement | None — accept all source columns |
| Partition | `ingest_date` (YYYY-MM-DD) |
| Retention | 90 days (Delta `logRetentionDuration`) |
| PII handling | Present — mask in Silver |

### Silver

| Property | Value |
|---|---|
| Write mode | MERGE (upsert on business key) |
| Schema enforcement | Strict (`mergeSchema = false`) |
| Partition | Derived from business date (e.g. `order_date`) |
| Deduplication | On business key + max `updated_at` |
| PII handling | Hash or null sensitive columns |

### Gold

| Property | Value |
|---|---|
| Write mode | OVERWRITE (full refresh) or MERGE (incremental) |
| Schema enforcement | Strict |
| Partition | None or low-cardinality (e.g. `region`) |
| Slowly changing | SCD Type 2 for dimensions |
| Surrogate keys | Integer sequence generated via `monotonically_increasing_id` + offset |

## Delta Configuration Defaults

```python
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "8")   # dev default; scale in test
```
