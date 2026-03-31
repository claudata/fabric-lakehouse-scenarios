# Demo Flow — 30–45 Minutes

A guided walkthrough demonstrating all 8 scenarios end-to-end. Designed for a live demo
or recorded walkthrough. Adjust timing per audience depth.

---

## Pre-flight Checklist (5 min before demo)

- [ ] Fabric workspace `dataeng-dev` is open with `lh_advanced_scenarios` attached
- [ ] Scenario 01 bootstrap notebook has been run (control tables exist)
- [ ] Seed data is loaded (`bronze.orders_raw` has rows)
- [ ] All 8 scenario notebooks are imported to the workspace

---

## Demo Script

### Block 1 — Architecture Overview (3 min)

Show the README architecture diagram. Briefly explain:
- OneLake + medallion layers
- Single Lakehouse, multiple Delta table schemas (bronze/silver/gold)
- How Data Factory, Spark notebooks, and the SQL endpoint connect

---

### Block 2 — Ingestion: Watch Data Arrive (5 min)

1. **Trigger** `pl_bronze_orders_ingest` manually.
2. Show the **pipeline monitor** while it runs.
3. After completion, run in the Notebook/SQL editor:

```sql
SELECT _ingest_date, COUNT(*) AS rows
FROM   bronze.orders_raw
GROUP  BY _ingest_date ORDER BY _ingest_date DESC;
```

4. Show the watermark table updated:

```sql
SELECT * FROM bronze.pipeline_watermark;
```

**Talking point:** Re-trigger the same pipeline — demonstrate 0 new rows (idempotency).

---

### Block 3 — Silver Transformation + DQ (7 min)

1. Open notebook `05_silver_orders_quality` (runs both DQ and Silver MERGE).
2. Before running, inject 2–3 bad rows manually (null `order_id`, negative `amount`).
3. **Run the notebook** — point out the DQ tagging cells.
4. After run:

```sql
-- Quarantined rows
SELECT order_id, _quarantine_reason FROM bronze.orders_quarantine LIMIT 5;

-- Silver is clean
SELECT status, COUNT(*), AVG(amount) FROM silver.orders GROUP BY status;
```

**Talking point:** PII hashing — show `customer_id_hash` (64-char hex), not raw email.

---

### Block 4 — CDC and SCD2 (8 min)

1. Show `silver.customers` — all rows, including `is_deleted`.
2. Run `04_gold_dim_customer_scd2` with `p_mode = 'incremental'`.
3. Simulate a region change in Silver, re-run, then show history:

```sql
SELECT customer_id, region, valid_from, valid_to, is_current
FROM   gold.dim_customer
WHERE  customer_id = 'C0003'
ORDER  BY valid_from;
```

**Talking point:** Expired row has `valid_to` set; new row has `is_current = true`.

---

### Block 5 — SQL Endpoint + Row-Level Security (5 min)

1. Open the SQL query editor on the `lh_advanced_scenarios` SQL endpoint.
2. Run `SELECT * FROM vw_kpi_dashboard ORDER BY order_month DESC LIMIT 10`.
3. Switch user context — show that `analyst-west` only sees WEST data.

**Talking point:** No Power BI required — SQL endpoint is queryable from any TDS tool.

---

### Block 6 — Performance Tuning (5 min)

1. Run `DESCRIBE DETAIL silver.orders` — show high file count, small avg size.
2. Run `OPTIMIZE silver.orders ZORDER BY (order_date, status)`.
3. Re-run `DESCRIBE DETAIL` — compare file count and average size.
4. Run the benchmark queries (before + after times from `07_perf_tuning` notebook).

**Talking point:** V-Order is applied on Fabric-native writes; ZORDER helps query pushdown.

---

### Block 7 — Observability & Alerts (5 min)

1. Open `08_observability_checks`.
2. Point out the `p_max_freshness_hours` parameter — set to `0` to force a freshness alert.
3. Run — show the alert row written to `bronze.pipeline_alerts`.

```sql
SELECT check_name, severity, message, raised_at
FROM   bronze.pipeline_alerts
ORDER  BY raised_at DESC
LIMIT  5;
```

4. Restore `p_max_freshness_hours = 25` and re-run — show alert remains (no auto-resolve
   unless a pipeline updates the Silver table).

---

### Block 8 — Validation Matrix (2 min)

Open [validation/validation-matrix.md](validation-matrix.md).  
Run the end-to-end SQLs from the Verification Checklist section.

All counts should match expectations — **demo complete**.

---

## Q&A Prompts

- "How does late-arriving CDC data get handled?" → Scenario 03 walkthrough
- "What happens to bad rows?" → Scenario 05 quarantine + replay
- "How do we prevent identity theft from raw PII?" → SHA-256 in Silver + RLS in Gold
- "What does this cost?" → Scenario 07 V-Order + OPTIMIZE reduces scan, lowers CU cost
