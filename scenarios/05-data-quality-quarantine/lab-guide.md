# Scenario 05 — Data Quality + Quarantine

**Phase:** 2 – Govern  
**Duration:** 60 min  
**Difficulty:** Advanced  
**Domain:** Generic (Orders)

---

## Learning Objectives

1. Define a rule-based data quality framework that runs inline during Silver transformation.
2. Route failing records to an isolated quarantine Delta table rather than dropping them silently.
3. Control pipeline behaviour on quality failure: `fail_fast`, `warn`, or `continue`.

## Prerequisites

- Scenario 01 complete: `bronze.orders_raw` exists.
- Scenario 02 complete: `silver.orders` exists (quarantine will complement it).

## Architecture

```
bronze.orders_raw
       │
       ▼
Notebook: 05_silver_orders_quality
  ├─ Apply DQ rules (nullable, range, regex, referential)
  ├─ Tag rows: passed / failed
  ├─ Route FAILED → Files/quarantine/orders/<date>/
  │                 bronze.orders_quarantine (Delta log of rejections)
  └─ Route PASSED → silver.orders  (MERGE, same as Scenario 02)
```

## Quality Rules

| Rule ID | Column | Check | Failure Mode |
|---|---|---|---|
| R01 | `order_id` | NOT NULL | `fail_fast` |
| R02 | `updated_at` | NOT NULL | `fail_fast` |
| R03 | `amount` | >= 0.00 | `warn` |
| R04 | `status` | IN ('NEW','SHIPPED','CANCELLED','RETURNED') | `warn` |
| R05 | `order_date` | Between 2000-01-01 and TODAY | `continue` |
| R06 | `customer_id` | NOT NULL | `fail_fast` |

Failure modes:
- **fail_fast** — halt the notebook immediately and log to `pipeline_errors`.
- **warn** — quarantine the row, emit a notebook warning, continue processing.
- **continue** — quarantine the row silently, continue.

## Step-by-Step Instructions

### Step 1 — Inject bad records into Bronze

```python
from pyspark.sql import Row
from decimal import Decimal

bad_rows = [
    Row(order_id=None,     customer_id="C1", order_date=None, updated_at=None,
        status="NEW",      amount=Decimal("10.00"), _ingest_date="2026-03-31"),  # R01+R02
    Row(order_id="ORD999", customer_id="C2", order_date=None, updated_at=...,
        status="DELETED",  amount=Decimal("-5.00"), _ingest_date="2026-03-31"), # R03+R04
    Row(order_id="ORD998", customer_id="C3", order_date=..., updated_at=...,
        status="NEW",      amount=Decimal("20.00"), _ingest_date="2026-03-31"), # clean
]
```

### Step 2 — Run the DQ notebook

Open **`05_silver_orders_quality`** and run all cells.

The DQ engine adds columns `_dq_passed` (boolean) and `_dq_failures` (array<string>)
to each row before routing.

### Step 3 — Inspect the quarantine table

```sql
SELECT order_id, _dq_failures, _quarantine_reason, _quarantine_ts
FROM   bronze.orders_quarantine
ORDER  BY _quarantine_ts DESC
LIMIT  20;
```

### Step 4 — Confirm Silver only received passing rows

```sql
SELECT COUNT(*) FROM silver.orders
WHERE order_id IS NULL OR amount < 0 OR status NOT IN ('NEW','SHIPPED','CANCELLED','RETURNED');
-- Expect 0
```

### Step 5 — Replay quarantined records

Correct a quarantined row and re-insert to Bronze's landing path, then re-run the pipeline.
The corrected row should clear quality and be written to Silver.

## Expected Outputs

| Artifact | Location | Check |
|---|---|---|
| Quarantine table | `bronze.orders_quarantine` | Rows with `_dq_failures` populated |
| Silver table | `silver.orders` | No rows violating R01–R06 |
| Quarantine files | `Files/quarantine/orders/<date>/` | Parquet files of rejected rows |
| Error log | `bronze.pipeline_errors` | `fail_fast` violations logged |

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| All rows quarantined | Rule condition inverted | Check `when(rule, True).otherwise(False)` logic |
| Quarantine table empty | Routes not configured | Verify filter `_dq_passed == False` paths |
| Pipeline aborts on R03 | R03 set to `fail_fast` | Change mode to `warn` in rules config |

## Challenge Extension

- Integrate with Microsoft Fabric Data Quality (preview) to register rules as managed checks.
- Produce a DQ summary row per pipeline run and write to `bronze.dq_run_summary` for trend monitoring.

## Acceptance Criteria

- [ ] `silver.orders` contains zero rows violating any DQ rule
- [ ] `bronze.orders_quarantine` contains all rejected rows with failure tags
- [ ] `fail_fast` rules halt the notebook before any Silver write
- [ ] Corrected quarantine rows successfully replay into Silver
