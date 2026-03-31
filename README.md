# Fabric Lakehouse — Advanced Build Scenarios

A hands-on, end-to-end scenario pack for Microsoft Fabric Lakehouse covering enterprise
data engineering patterns across ingestion, transformation, governance, serving, and operations.

## Architecture: Medallion Lakehouse

```
Sources ──► Bronze (raw) ──► Silver (cleansed) ──► Gold (curated) ──► SQL Endpoint / Semantic Model
              │                   │                     │
         Data Factory         Spark Notebooks       Spark Notebooks
         pipelines            (Delta write)         (Delta MERGE)
```

## Prerequisites

| Requirement | Detail |
|---|---|
| Microsoft Fabric capacity | F4 or above (F64 recommended for performance labs) |
| Workspace | Dev workspace + optional Test workspace |
| Lakehouse | Single Lakehouse: `lh_advanced_scenarios` |
| Runtime | Spark runtime 1.3 (Spark 3.5 / Delta 3.x) |
| Managed identity | System-assigned MI on the Fabric workspace |
| Storage | OneLake (default); no ADLS Gen2 attachment required |

## Workspace Conventions

See [baseline/workspace-conventions.md](baseline/workspace-conventions.md).

## Scenarios

| # | Scenario | Phase | Duration |
|---|---|---|---|
| 01 | [Batch Incremental Ingestion](scenarios/01-batch-incremental-ingestion/lab-guide.md) | 1 – Ingest | 45 min |
| 02 | [Spark Silver Transformation](scenarios/02-spark-silver-transformation/lab-guide.md) | 1 – Transform | 45 min |
| 03 | [CDC + Upsert with MERGE](scenarios/03-cdc-upsert/lab-guide.md) | 2 – CDC | 60 min |
| 04 | [SCD Type 2 Gold Dimensions](scenarios/04-scd-type2/lab-guide.md) | 2 – Model | 60 min |
| 05 | [Data Quality + Quarantine](scenarios/05-data-quality-quarantine/lab-guide.md) | 2 – Govern | 60 min |
| 06 | [SQL Endpoint Serving Layer](scenarios/06-sql-endpoint-serving/lab-guide.md) | 3 – Serve | 30 min |
| 07 | [Performance & Cost Tuning](scenarios/07-performance-tuning/lab-guide.md) | 3 – Optimize | 60 min |
| 08 | [Observability & Reliability](scenarios/08-observability-reliability/lab-guide.md) | 3 – Operate | 45 min |

## Excluded (out of scope)

- Power BI semantic modeling / report building
- Real-Time Analytics (Eventhouse / KQL)
- Data Activator

## Demo Flow (30–45 min)

See [validation/demo-flow.md](validation/demo-flow.md).
