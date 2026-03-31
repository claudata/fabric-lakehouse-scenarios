# Scenario 07 — Performance & Cost Tuning

**Phase:** 3 – Optimize  
**Duration:** 60 min  
**Difficulty:** Advanced  
**Domain:** Generic

---

## Learning Objectives

1. Profile Spark jobs with the Fabric Monitoring Hub and identify skew, spill, and small-file problems.
2. Apply `OPTIMIZE` with `ZORDER BY` and `VACUUM` to compact files and reduce scan cost.
3. Right-size shuffle partitions and executor configuration for the actual data volumes.

## Prerequisites

- Scenarios 01–05 complete: Bronze, Silver, and Gold tables must be populated.
- At least 5 Data Factory pipeline runs completed (to accumulate small files).

## Architecture

The tuning lab is a standalone maintenance notebook with parameterised targets. It does not
alter the medallion table schemas — only file layout and Spark configuration.

```
Existing Delta tables
        │
        ▼
Notebook: 07_perf_tuning
  ├─ Phase A: Profile — collect file sizes, partition counts, Spark event metrics
  ├─ Phase B: OPTIMIZE + ZORDER — compact small files, co-locate query columns
  ├─ Phase C: VACUUM — purge obsolete Delta log files
  └─ Phase D: Benchmark — before / after query latency comparison
```

## Key Metrics to Collect

| Metric | Where | Target |
|---|---|---|
| Avg file size per table | `DESCRIBE DETAIL` | 64–128 MB |
| Small files (< 10 MB) | `DESCRIBE DETAIL.numFiles` vs avg | < 5% of files |
| Shuffle read bytes | Spark UI / Event log | Minimise cross-executor shuffle |
| SQL endpoint scan time | Fabric Query Activity | ≤ 2 s for typical aggregation |
| VACUUM freed bytes | VACUUM output | Quantify storage recovered |

## Step-by-Step Instructions

### Step A — Profile before tuning

Run **`07_perf_tuning`** Phase A cells. They output a table like:

| Table | Num Files | Avg File MB | Min File MB | Max File MB |
|---|---|---|---|---|
| bronze.orders_raw | 142 | 1.2 | 0.01 | 8.4 |
| silver.orders | 28 | 0.9 | 0.01 | 3.1 |
| gold.dim_customer | 6 | 0.4 | 0.1 | 0.9 |

Note the high file count and low average sizes — a classic small-file problem.

### Step B — Run OPTIMIZE with ZORDER

```sql
-- Compact Silver orders and Z-order on most-filtered columns
OPTIMIZE lh_advanced_scenarios.silver.orders
ZORDER BY (order_date, status);

-- Compact Gold dimension
OPTIMIZE lh_advanced_scenarios.gold.dim_customer
ZORDER BY (region, is_current);
```

### Step C — VACUUM

```sql
-- Retain 7 days (168 hours) of Delta history
VACUUM lh_advanced_scenarios.silver.orders RETAIN 168 HOURS;
VACUUM lh_advanced_scenarios.gold.dim_customer RETAIN 168 HOURS;
VACUUM lh_advanced_scenarios.bronze.orders_raw RETAIN 168 HOURS;
```

> **Safety note:** The default retention is 7 days. Do not reduce below 168 hours
> unless you have confirmed no active streaming readers or time-travel queries.

### Step D — Benchmark after tuning

Re-run the benchmark queries:

```sql
-- Aggregation benchmark
SELECT status, COUNT(*), SUM(amount)
FROM   silver.orders
WHERE  order_date BETWEEN '2025-01-01' AND '2025-12-31'
GROUP  BY status;
```

Compare execution plans in Spark UI: look for reduced number of input files and
lower shuffle exchange bytes.

### Step E — Right-size Spark configuration

Based on profiling results, update the workspace Environment or notebook header:

```python
# Tuned for the actual Silver orders volume (~50 MB post-OPTIMIZE)
spark.conf.set("spark.sql.shuffle.partitions", "4")   # down from 200 default
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.memory", "14g")
```

## Expected Outcomes

| Before | After |
|---|---|
| 142 files, avg 1.2 MB | ~8 files, avg 64 MB |
| Scan time ~8 s | Scan time ≤ 2 s |
| Shuffle partitions: 200 | Shuffle partitions: 4 |

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| VACUUM fails | Concurrent streaming job | Stop streaming, run VACUUM, restart |
| OPTIMIZE slow | Too many small files | Increase executor memory; run OPTIMIZE in batches by partition |
| Query still slow after ZORDER | Filter not on Z-ordered columns | Re-run OPTIMIZE ZORDER BY the actual filter columns |

## Challenge Extension

- Schedule a weekly `OPTIMIZE + VACUUM` maintenance pipeline using Data Factory with a `ForEach` over all managed Delta tables.
- Compare V-Order (Fabric-native write optimization) vs ZORDER scan latency using identical queries.

## Acceptance Criteria

- [ ] Profile report shows file count and average file sizes before and after
- [ ] Post-OPTIMIZE average file size ≥ 32 MB for Silver and Gold tables
- [ ] VACUUM run completes without error, log shows freed bytes
- [ ] Benchmark query latency reduced by ≥ 40% compared to pre-OPTIMIZE baseline
