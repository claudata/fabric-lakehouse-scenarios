# Scenario 08 — Observability & Reliability

**Phase:** 3 – Operate  
**Duration:** 45 min  
**Difficulty:** Advanced  
**Domain:** Generic

---

## Learning Objectives

1. Emit structured telemetry from notebooks and pipelines to a Delta log table.
2. Simulate failures (source outage, schema drift, timeout) and verify recovery runbooks.
3. Enforce SLA/SLO checks that alert when pipeline latency or data freshness thresholds are breached.

## Prerequisites

- Scenarios 01–02 complete: ingestion and Silver pipelines are operational.
- Control table `bronze.pipeline_errors` created in Scenario 01 bootstrap.

## Architecture

```
Every pipeline / notebook
       │  (emit structured run event)
       ▼
bronze.pipeline_run_log  ← append-only telemetry table
       │
       ▼
Notebook: 08_observability_checks
  ├─ SLA check: last successful run within expected window?
  ├─ Freshness check: Silver/Gold updated_at <= max allowed lag?
  ├─ Error rate: failures / total runs in last 24h > threshold?
  └─ Alert: log to bronze.pipeline_alerts + (optional) Teams webhook
```

## Telemetry Schema

```sql
CREATE TABLE bronze.pipeline_run_log (
    run_id          STRING     NOT NULL,
    pipeline_name   STRING     NOT NULL,
    entity_name     STRING,
    status          STRING     NOT NULL,   -- STARTED | SUCCEEDED | FAILED | SKIPPED
    rows_read       BIGINT,
    rows_written    BIGINT,
    rows_quarantine BIGINT,
    error_message   STRING,
    started_at      TIMESTAMP,
    ended_at        TIMESTAMP,
    duration_s      DOUBLE
) USING DELTA
```

## Step-by-Step Instructions

### Step 1 — Bootstrap telemetry table

Run the first two cells of **`08_observability_checks`** to create `bronze.pipeline_run_log`
and `bronze.pipeline_alerts` if they don't already exist.

### Step 2 — Add telemetry to an existing notebook

Open notebook `02_silver_orders_transform`. Add cells to emit a `STARTED` event at the top
and a `SUCCEEDED` event at the end:

```python
from uuid import uuid4
from datetime import datetime, timezone

RUN_ID = str(uuid4())
_run_log_schema = "lh_advanced_scenarios.bronze.pipeline_run_log"

def log_run(status, rows_read=None, rows_written=None, rows_q=None, error=None):
    now = datetime.now(timezone.utc)
    spark.createDataFrame([{
        "run_id":          RUN_ID,
        "pipeline_name":   "02_silver_orders_transform",
        "entity_name":     "orders",
        "status":          status,
        "rows_read":       rows_read,
        "rows_written":    rows_written,
        "rows_quarantine": rows_q,
        "error_message":   error,
        "started_at":      started_at if status != "STARTED" else now,
        "ended_at":        now if status != "STARTED" else None,
        "duration_s":      (now - started_at).total_seconds() if status != "STARTED" else None,
    }]).write.format("delta").mode("append").saveAsTable(_run_log_schema)

started_at = datetime.now(timezone.utc)
log_run("STARTED")
```

### Step 3 — Simulate a source outage

Set `p_landing_path` to a non-existent path and run pipeline `pl_bronze_orders_ingest`.
Verify:
- Pipeline fails at the Copy Activity
- `OnFailureAlert` activity fires
- Error record appears in `bronze.pipeline_errors`

### Step 4 — Simulate schema drift

Add a new column to the landing CSV and append to Bronze with `mergeSchema = false`.
Verify the write fails and the error is logged.

### Step 5 — Run the observability checks notebook

**`08_observability_checks`** runs SLA checks:

| Check | Logic | Threshold |
|---|---|---|
| Freshness | `MAX(_silver_ts)` in `silver.orders` vs `CURRENT_TIMESTAMP()` | > 25 hours → WARN |
| Run success rate | failures / total in last 24h in `pipeline_run_log` | > 10% → ALERT |
| SLA latency | avg `duration_s` for last 5 runs | > 600 s → WARN |
| Row count anomaly | today's write ÷ 7-day avg | < 50% or > 200% → ALERT |

Alerts are written to `bronze.pipeline_alerts` and optionally POSTed to a Teams webhook.

## Expected Outputs

| Artifact | Location | Check |
|---|---|---|
| Run log | `bronze.pipeline_run_log` | Every run has STARTED + terminal event |
| Error log | `bronze.pipeline_errors` | Simulated failures captured |
| Alerts | `bronze.pipeline_alerts` | Freshness / error rate alert visible |

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Run log has `STARTED` but no terminal event | Exception before `log_run("SUCCEEDED")` | Wrap notebook body in try/except/finally |
| Teams webhook returns 400 | Payload format incorrect | Check Adaptive Card schema |
| SLA always green even after failure | Threshold too loose | Tighten `max_lag_hours` parameter |

## Challenge Extension

- Build a Fabric Monitoring Dashboard notebook that queries `pipeline_run_log` and renders
  a Pandas/matplotlib trend chart of daily run durations and error rates.
- Wire the alerts into a Data Activator reflex to auto-trigger a remediation pipeline when
  data freshness exceeds 30 hours.

## Acceptance Criteria

- [ ] Every pipeline run produces a `STARTED` + `SUCCEEDED` / `FAILED` pair in `pipeline_run_log`
- [ ] Simulated source outage produces an error record in `pipeline_errors`
- [ ] Freshness check correctly detects a stale Silver table
- [ ] Error rate check triggers an alert row in `pipeline_alerts`
- [ ] Re-running after a fix clears the alert (alert row status = RESOLVED)
