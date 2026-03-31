# Scenario 02 — Spark Silver Transformation

**Phase:** 1 – Transform  
**Duration:** 45 min  
**Difficulty:** Intermediate  
**Domain:** Generic (Orders)

---

## Learning Objectives

1. Apply schema enforcement, data typing, and null handling to raw Bronze data.
2. Produce a deduplicated Silver Delta table using a MERGE upsert on the business key.
3. Hash PII columns (customer_id) before the write.

## Prerequisites

- **Scenario 01 complete:** `bronze.orders_raw` must exist with at least one partition.

## Architecture

```
bronze.orders_raw  (Delta, append-only, partitioned by _ingest_date)
        │
        ▼
Notebook: 02_silver_orders_transform
  ├─ Read incremental Bronze partition(s)
  ├─ Cast / validate types
  ├─ Hash PII
  ├─ Deduplicate on (order_id, max updated_at)
  └─ MERGE INTO silver.orders
```

## Dataset

Input: `bronze.orders_raw` (from Scenario 01).  
Output: `silver.orders`

| Column | Type | Transformation |
|---|---|---|
| order_id | STRING | No change |
| customer_id_hash | STRING | SHA-256 of raw customer_id |
| order_date | DATE | Cast + null check |
| updated_at | TIMESTAMP | No change |
| status | STRING | `TRIM + UPPER` |
| amount | DECIMAL(10,2) | Null → 0.00; negative → quarantine |
| _silver_ts | TIMESTAMP | `current_timestamp()` |

## Step-by-Step Instructions

### Step 1 — Review the Bronze schema

```sql
DESCRIBE TABLE lh_advanced_scenarios.bronze.orders_raw;
```

Confirm column names, types, and that `updated_at` is a proper Timestamp.

### Step 2 — Run the Silver transformation notebook

Open **`02_silver_orders_transform`**. The notebook:

1. Reads Bronze rows where `_ingest_date >= last silver watermark`.
2. Applies type casts, trims, PII hashing.
3. Deduplicates: keeps the row with `max(updated_at)` per `order_id`.
4. MERGEs into `silver.orders` on `order_id`.

### Step 3 — Validate the Silver table

```sql
-- Uniqueness on business key
SELECT order_id, COUNT(*) AS cnt
FROM   silver.orders
GROUP  BY order_id
HAVING cnt > 1;
-- Expect 0 rows

-- PII removed
SELECT customer_id_hash, LENGTH(customer_id_hash) AS hash_len
FROM   silver.orders
LIMIT  5;
-- Expect 64-char hex strings
```

### Step 4 — Run idempotency check

Re-run the notebook against the same Bronze partition. Row count in `silver.orders` must
not increase; Delta history shows a MERGE with 0 inserted rows.

## Expected Outputs

| Artifact | Location | Check |
|---|---|---|
| Silver table | `silver.orders` | Unique on `order_id` |
| PII column | `customer_id_hash` | SHA-256 hex, 64 chars |
| Delta history | MERGE entry | `operationMetrics.numTargetRowsInserted + Updated = source rows` |

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Duplicate order_ids in silver | MERGE condition wrong | Check `ON s.order_id = t.order_id` |
| `customer_id_hash` is null | Bronze `customer_id` is null | Add null-safe hash or quarantine |
| Amount negative rows still present | Quarantine path not set up | See Scenario 05 |

## Challenge Extension

- Add a column `days_to_ship` computed as `DATEDIFF(ship_date, order_date)` (add `ship_date` to the seed).
- Implement a Silver-to-Silver micro-batch using Spark Structured Streaming with `trigger(availableNow=True)`.

## Acceptance Criteria

- [ ] `silver.orders` contains no duplicate `order_id` rows
- [ ] `customer_id_hash` is a 64-character hex string (SHA-256)
- [ ] Delta MERGE history entry exists with correct operation metrics
- [ ] Re-run produces 0 new inserted rows in silver
