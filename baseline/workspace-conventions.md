# Workspace Conventions

## Naming

| Artifact | Pattern | Example |
|---|---|---|
| Workspace | `<team>-<env>` | `dataeng-dev`, `dataeng-test` |
| Lakehouse | `lh_<domain>_<purpose>` | `lh_advanced_scenarios` |
| Pipeline | `pl_<layer>_<source>_<action>` | `pl_bronze_orders_ingest` |
| Notebook | `<nn>_<layer>_<description>` | `02_silver_orders_transform` |
| Delta table | `<layer>.<entity>` | `silver.orders` |
| Parameter names | `snake_case` | `watermark_value` |

## Environments

| Env | Workspace | Purpose |
|---|---|---|
| dev | `dataeng-dev` | Active development, schema experimentation |
| test | `dataeng-test` | Integration testing, UAT, perf benchmarking |

Promote artifacts from dev → test via Fabric Deployment Pipelines. Do **not** run tests directly
in a shared prod workspace.

## Git Integration

- Connect both workspaces to separate branches: `main` (prod) and `dev`.
- Commit pipeline JSON, notebook definitions, and SQL scripts.
- Never commit Lakehouse data or secret values.

## Secrets Management

- Use Fabric **Environment** secrets or Azure Key Vault references.
- Never hard-code connection strings or SAS tokens in notebooks or pipelines.

## Capacity and Cost Guard-rails

- Default notebook attach policy: use the workspace capacity, not a personal trial.
- Set a `spark.executor.instances` cap of 4 for dev and 8 for test to avoid runaway costs.
- All pipelines include a failure-alert activity (see Scenario 08).
