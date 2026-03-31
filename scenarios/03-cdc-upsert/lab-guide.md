# Scenario 03 — CDC + Upsert with MERGE

**Phase:** 2 – CDC  
**Duration:** 60 min  
**Difficulty:** Advanced  
**Domain:** Generic (Customers)

---

## Learning Objectives

1. Implement a full Change Data Capture (CDC) pattern using operation flags (`I`, `U`, `D`).
2. Apply Delta Lake `MERGE` for upserts and logical deletes without physical row removal.
3. Handle late-arriving change records and guarantee replay-safe, idempotent processing.

## Prerequisites

- Scenario 01 complete (`bronze.orders_raw` exists).
- `silver.customers` does not need to exist yet — the notebook creates it on first run.

## Architecture

```
Source CDC feed (CSV with op_flag)
        │
        ▼
bronze.customers_cdc    ← append-only CDC log (Bronze)
        │
        ▼
Notebook: 03_silver_customers_cdc
  ├─ Resolve ordering: window by (customer_id, change_seq DESC)
  ├─ MERGE INSERT/UPDATE (op_flag IN ('I','U'))
  ├─ MERGE soft-DELETE  (op_flag = 'D' → set is_deleted = true)
  └─ silver.customers
```

## Dataset

Synthetic CDC feed with operation flags.

| Column | Type | Notes |
|---|---|---|
| customer_id | STRING | Business key |
| name | STRING | Full name |
| email | STRING | PII — hashed in Silver |
| region | STRING | |
| op_flag | STRING | `I`=Insert, `U`=Update, `D`=Delete |
| change_seq | LONG | Monotonically increasing per source |
| change_ts | TIMESTAMP | Source system timestamp |

Late-arriving records have a lower `change_seq` than already-processed records.

## Step-by-Step Instructions

### Step 1 — Ingest CDC batch to Bronze

Run the seed ingestion:

```python
# In a scratch cell — run once
spark.read.csv("Files/landing/customers_cdc/seed/customers_cdc_seed.csv", header=True) \
     .write.format("delta").mode("overwrite") \
     .option("overwriteSchema", "true") \
     .saveAsTable("lh_advanced_scenarios.bronze.customers_cdc")
```

### Step 2 — Understand the late-arriving data problem

Inspect the seed:

```sql
SELECT customer_id, op_flag, change_seq, change_ts
FROM   bronze.customers_cdc
WHERE  customer_id = 'C0001'
ORDER  BY change_seq;
-- Expect rows out of natural timestamp order demonstrating late arrival
```

### Step 3 — Run the CDC processing notebook

Open **`03_silver_customers_cdc`** and run all cells. It:

1. Reads all unprocessed CDC rows (change_seq > last processed seq in watermark table).
2. Applies a window to keep only the **last** operation per `customer_id` in this micro-batch.
3. Splits into upsert set and delete set.
4. Issues a single MERGE per set — insert/update rows, then soft-delete rows.

### Step 4 — Simulate a late-arriving update

Append a row with a lower `change_seq` than already processed:

```python
from pyspark.sql import Row
from datetime import datetime, timezone

late_row = spark.createDataFrame([Row(
    customer_id="C0001",
    name="Alice Corrected",
    email="alice@example.com",
    region="WEST",
    op_flag="U",
    change_seq=2,          # earlier than already-processed seq=50
    change_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
)])
late_row.write.format("delta").mode("append").saveAsTable("lh_advanced_scenarios.bronze.customers_cdc")
```

Re-run the notebook. Confirm the late row is **not** applied (Silver keeps the higher-seq state).

### Step 5 — Simulate a delete

```python
delete_row = spark.createDataFrame([Row(
    customer_id="C0002", name=None, email=None, region=None,
    op_flag="D", change_seq=9999,
    change_ts=datetime.now(timezone.utc),
)])
delete_row.write.format("delta").mode("append").saveAsTable("lh_advanced_scenarios.bronze.customers_cdc")
```

Re-run. Confirm `silver.customers` has `is_deleted = true` for C0002.

## Expected Outputs

| Artifact | Location | Check |
|---|---|---|
| Bronze CDC log | `bronze.customers_cdc` | All raw CDC rows (append-only) |
| Silver table | `silver.customers` | Unique on `customer_id`, has `is_deleted` flag |
| Email PII | `email_hash` | SHA-256 hex |
| Soft deletes | `is_deleted = true` | C0002 is marked, not physically removed |

## Idempotency Check

```sql
SELECT customer_id, COUNT(*) c FROM silver.customers GROUP BY customer_id HAVING c>1;
-- Expect 0 rows
```

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Late row overwrites current state | Window not ordering by `change_seq DESC` | Fix the window spec |
| Deletes not applied | MERGE missing `WHEN MATCHED AND op='D'` branch | Add the soft-delete branch |
| Duplicates after re-run | Watermark not updated | Check watermark update logic |

## Challenge Extension

- Add a **hard-delete** mode controlled by a notebook parameter that physically removes
  rows older than 90 days (GDPR right-to-erasure pattern).
- Publish the `silver.customers` table with Row Access Policies so only `WEST` region
  data is visible to west-region analysts.

## Acceptance Criteria

- [ ] `silver.customers` is unique on `customer_id`
- [ ] Logical deletes result in `is_deleted = true`, row remains in table
- [ ] Late-arriving rows with lower `change_seq` do not overwrite current state
- [ ] Re-run of the same CDC batch makes no changes (idempotent)
