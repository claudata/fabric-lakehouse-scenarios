# Scenario Template

Copy this template for every new scenario. Fill in each section before writing code.

---

## Scenario NN — Title

**Phase:** [1 – Ingest | 2 – Transform/Govern | 3 – Serve/Optimize/Operate]  
**Duration:** XX min  
**Difficulty:** [Intermediate | Advanced]  
**Domain:** Generic / Financial Services / Retail / Healthcare

---

### Learning Objectives

By the end of this scenario you will be able to:

1. …
2. …
3. …

### Prerequisites

- Shared baseline complete (see [baseline/](../baseline/))
- Scenario NN-1 outputs available (state which tables/paths)
- List any additional packages or connections required

### Architecture Diagram

```
[Source] ──► [Pipeline / Notebook] ──► [Delta Table: layer.table_name]
```

### Dataset

| Field | Detail |
|---|---|
| Source | Synthetic / Open data / Prior scenario output |
| Volume | ~X rows / X MB |
| Schema | Described inline in the lab guide |
| Latency profile | Batch / Micro-batch / Streaming |

### Step-by-Step Instructions

#### Step 1 — …

…

#### Step 2 — …

…

### Expected Outputs

| Artifact | Location | Validation query |
|---|---|---|
| Delta table | `bronze.table_name` | `SELECT COUNT(*) FROM bronze.table_name` |

### Idempotency Check

Re-run the pipeline/notebook and confirm row counts and record identity are unchanged.

### Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| … | … | … |

### Challenge Extension

- …

### Acceptance Criteria

- [ ] Pipeline/notebook completes without error
- [ ] Row count matches expectation
- [ ] Delta history shows a single WRITE/MERGE/UPDATE operation per run
- [ ] Re-run is idempotent (no duplicates, no extra versions)
