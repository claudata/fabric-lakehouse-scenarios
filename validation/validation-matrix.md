# Validation Matrix

Cross-reference of acceptance criteria and skills demonstrated for each scenario.

## Matrix

| Scenario | Skill Demonstrated | Key Acceptance Check | Idempotent? | Tooling Used |
|---|---|---|---|---|
| 01 – Batch Ingestion | Watermarking, incremental copy | Bronze partition row count matches source | Yes | Data Factory, Spark |
| 02 – Silver Transform | Schema enforcement, PII hashing, MERGE | `silver.orders` unique on `order_id` | Yes | Spark, Delta MERGE |
| 03 – CDC + Upsert | CDC patterns, soft delete, late-arrival | Late row does not overwrite; D→`is_deleted=true` | Yes | Spark, Delta MERGE |
| 04 – SCD Type 2 | Surrogate keys, effective dates | Exactly 1 `is_current=true` per customer | Yes | Spark, Delta 2-pass MERGE |
| 05 – Data Quality | Rule engine, quarantine routing, fail_fast | Silver has zero DQ violations | Yes | Spark, Delta |
| 06 – SQL Serving | Views, Row-Level Security | Analyst sees only own-region rows | N/A | T-SQL, SQL endpoint |
| 07 – Performance | OPTIMIZE, ZORDER, VACUUM, benchmarking | ≥40% latency reduction post-optimize | N/A | Spark SQL, Delta |
| 08 – Observability | Telemetry, SLA checks, alerting | All failures captured; freshness alert fires | Yes | Spark, Delta, Teams |

## End-to-End Dependency Chain

```
01 (Bronze) ──► 02 (Silver orders) ──► 05 (DQ gate) ──► 06 (SQL views)
                                                         └──► 07 (Perf tuning)
01 (Bronze) ──► 03 (Silver customers) ──► 04 (Gold dim_customer)
                                                         └──► 06 (SQL views)
All scenarios ──► 08 (Observability)
```

## Shared Control Tables (created in Scenario 01 bootstrap)

| Table | Purpose | Created By |
|---|---|---|
| `bronze.pipeline_watermark` | Stores last processed watermark per entity | Scenario 01 bootstrap |
| `bronze.pipeline_errors` | Structured error log | Scenario 01 bootstrap |
| `bronze.pipeline_run_log` | Pipeline telemetry | Scenario 08 bootstrap |
| `bronze.pipeline_alerts` | SLA / quality alerts | Scenario 08 bootstrap |
| `bronze.orders_quarantine` | DQ-rejected order rows | Scenario 05 |

## Verification Checklist

Run these queries after all scenarios to confirm the full end-to-end state.

```sql
-- 1. Control tables exist
SHOW TABLES IN lh_advanced_scenarios.bronze;

-- 2. Bronze populated
SELECT COUNT(*) FROM lh_advanced_scenarios.bronze.orders_raw;         -- > 10,000

-- 3. Silver clean (no nulls on key columns, no duplicates)
SELECT COUNT(*) FROM lh_advanced_scenarios.silver.orders WHERE order_id IS NULL;  -- 0
SELECT order_id, COUNT(*) c FROM lh_advanced_scenarios.silver.orders
GROUP BY order_id HAVING c > 1;                                                    -- 0 rows

-- 4. SCD2 integrity
SELECT customer_id, SUM(CAST(is_current AS INT)) c FROM lh_advanced_scenarios.gold.dim_customer
GROUP BY customer_id HAVING c > 1;                                                 -- 0 rows

-- 5. Quarantine populated (only if bad data was injected)
SELECT COUNT(*) FROM lh_advanced_scenarios.bronze.orders_quarantine;

-- 6. Views accessible
SELECT TOP 5 * FROM vw_kpi_dashboard;

-- 7. Telemetry present
SELECT pipeline_name, status, COUNT(*) FROM lh_advanced_scenarios.bronze.pipeline_run_log
GROUP BY pipeline_name, status ORDER BY pipeline_name;
```
