# Scenario 04 — SCD Type 2 Gold Dimensions

**Phase:** 2 – Model  
**Duration:** 60 min  
**Difficulty:** Advanced  
**Domain:** Generic (Customer dimension)

---

## Learning Objectives

1. Implement Slowly Changing Dimension Type 2 (SCD2) pattern in Delta Lake.
2. Manage surrogate keys, effective dates, and `is_current` flags without a sequence generator service.
3. Expire old rows and insert new versions atomically using a two-pass MERGE.

## Prerequisites

- Scenario 03 complete: `silver.customers` exists and is populated.

## Architecture

```
silver.customers  (CDC-applied, has is_deleted flag)
        │
        ▼
Notebook: 04_gold_dim_customer_scd2
  ├─ Detect changed attributes vs current Gold row
  ├─ Pass 1 MERGE: expire old rows (set valid_to, is_current=false)
  ├─ Pass 2 INSERT: new version rows (new surrogate key, valid_from=today)
  └─ gold.dim_customer
```

## SCD2 Table Schema

```sql
CREATE TABLE gold.dim_customer (
    customer_sk      BIGINT   NOT NULL,   -- surrogate key
    customer_id      STRING   NOT NULL,   -- business key
    name             STRING,
    email_hash       STRING,
    region           STRING,
    valid_from       DATE     NOT NULL,
    valid_to         DATE,                -- NULL = current row
    is_current       BOOLEAN  NOT NULL,
    is_deleted       BOOLEAN  NOT NULL,
    _gold_ts         TIMESTAMP
) USING DELTA
```

## Step-by-Step Instructions

### Step 1 — First load (full snapshot)

Run notebook **`04_gold_dim_customer_scd2`** with `p_mode = 'init'`.

All current (non-deleted) `silver.customers` rows are inserted as initial SCD2 rows:
- `valid_from = current_date()`
- `valid_to = NULL`
- `is_current = true`
- `customer_sk` = monotonically_increasing_id + 1000000 (offset to avoid 0)

### Step 2 — Inspect the initial load

```sql
SELECT customer_sk, customer_id, name, region, valid_from, valid_to, is_current
FROM   gold.dim_customer
LIMIT  10;
```

### Step 3 — Simulate a Silver attribute change

Update a customer's region in Silver (simulating a processed CDC update):

```sql
UPDATE silver.customers
SET    region = 'EAST', change_ts = CURRENT_TIMESTAMP(), change_seq = 99999
WHERE  customer_id = 'C0003';
```

### Step 4 — Run incremental SCD2

Re-run the notebook with `p_mode = 'incremental'`. The logic:

1. JOIN `silver.customers` with current Gold rows.
2. Flag rows where any tracked attribute differs.
3. **Pass 1 MERGE**: for changed rows, set `valid_to = yesterday`, `is_current = false`.
4. **Pass 2 INSERT**: insert new version with `valid_from = today`, `is_current = true`.

### Step 5 — Verify the SCD2 history

```sql
SELECT customer_id, name, region, valid_from, valid_to, is_current
FROM   gold.dim_customer
WHERE  customer_id = 'C0003'
ORDER  BY valid_from;
-- Expect 2 rows: one expired, one current
```

### Step 6 — Handle deleted customers

A customer with `silver.customers.is_deleted = true` should result in a Gold row where
`is_deleted = true` and `is_current = false`. Verify:

```sql
SELECT * FROM gold.dim_customer WHERE is_deleted = true;
```

## Expected Outputs

| Artifact | Location | Check |
|---|---|---|
| Dim table | `gold.dim_customer` | Has `customer_sk` surrogate key |
| SCD2 history | `gold.dim_customer` | Changed customer has 2 rows: 1 expired + 1 current |
| Current rows | `WHERE is_current = true` | Exactly 1 per `customer_id` |
| Surrogate key | `customer_sk` | Unique, no null, integer |

## Idempotency Check

```sql
SELECT customer_id, SUM(CAST(is_current AS INT)) curr_count
FROM   gold.dim_customer
GROUP  BY customer_id
HAVING curr_count > 1;
-- Expect 0 rows
```

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Every row shows `is_current = false` | Pass 2 INSERT not running | Check INSERT after MERGE logic |
| Duplicate `customer_sk` | Surrogate generation ran twice | Use `MAX(customer_sk) + 1` for increment logic |
| Unchanged rows get new version | Change detection predicate wrong | Compare only tracked columns, not `_silver_ts` |

## Challenge Extension

- Track SCD2 history for `region` only; treat `name` changes as SCD Type 1 (in-place updates).
- Build a `gold.dim_date` table (calendar dimensions) using a date range generator notebook.

## Acceptance Criteria

- [ ] `gold.dim_customer` has exactly 1 `is_current = true` row per active `customer_id`
- [ ] Changed customer has ≥ 2 historical rows in Gold
- [ ] `valid_to` is set on expired rows; `valid_to = NULL` on current rows
- [ ] Deleted customers have `is_deleted = true, is_current = false`
- [ ] Surrogate keys are unique integers with no nulls
