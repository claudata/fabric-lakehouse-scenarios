# Scenario 06 — SQL Endpoint Serving Layer

**Phase:** 3 – Serve  
**Duration:** 30 min  
**Difficulty:** Intermediate  
**Domain:** Generic

---

## Learning Objectives

1. Create curated SQL views over Gold Delta tables exposed through the Lakehouse SQL endpoint.
2. Apply Row-Level Security (RLS) using a Row Access Policy on the SQL endpoint.
3. Validate access behaviour for two test personas (analyst-west, analyst-east).

## Prerequisites

- Scenario 04 complete: `gold.dim_customer` exists.
- Scenario 02 complete: `silver.orders` exists (used as `fact_sales` proxy).
- Two Entra ID test users or service principals:  
  - `analyst-west@contoso.com` (WEST region access only)  
  - `analyst-east@contoso.com` (EAST region access only)

## Architecture

```
gold.dim_customer   silver.orders
        │                 │
        └────────┬─────────┘
                 ▼
     SQL endpoint views (T-SQL)
       ├─ vw_customer_summary
       ├─ vw_order_summary
       └─ vw_sales_by_region   (RLS-enforced)
```

## Step-by-Step Instructions

### Step 1 — Create the serving views

Connect to the SQL endpoint using the **SQL query editor** in Fabric or any tool supporting
TDS connections, and run [sql/serving_views.sql](sql/serving_views.sql).

### Step 2 — Create the RLS security mapping table

```sql
CREATE TABLE gold.rls_region_access (
    principal_name  NVARCHAR(200) NOT NULL,
    region          NVARCHAR(50)  NOT NULL
);

INSERT INTO gold.rls_region_access VALUES
    ('analyst-west@contoso.com', 'WEST'),
    ('analyst-east@contoso.com', 'EAST');
```

### Step 3 — Apply the Row Access Policy

```sql
-- Create the filter predicate
CREATE FUNCTION dbo.fn_rls_region_filter(@region NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
    SELECT 1 AS access_result
    WHERE  @region IN (
               SELECT region
               FROM   gold.rls_region_access
               WHERE  principal_name = USER_NAME()
           )
    OR     IS_MEMBER('db_owner') = 1;

-- Bind to the customer view source
CREATE SECURITY POLICY rls_customer_region
ADD FILTER PREDICATE dbo.fn_rls_region_filter(region)
ON gold.dim_customer
WITH (STATE = ON);
```

### Step 4 — Test with analyst-west

Connect as `analyst-west@contoso.com` and run:

```sql
SELECT region, COUNT(*) FROM vw_customer_summary GROUP BY region;
-- Expect: only WEST rows
```

### Step 5 — Test with analyst-east

Connect as `analyst-east@contoso.com` and run the same query.  
Expect: only EAST rows.

## Expected Outputs

| View | Serves | RLS |
|---|---|---|
| `vw_customer_summary` | Active (non-deleted) customer counts by region | Yes |
| `vw_order_summary` | Order totals by status | No |
| `vw_sales_by_region` | Revenue and order count by region × month | Regional (via dim_customer join) |

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| All rows visible regardless of user | Security Policy not enabled | `ALTER SECURITY POLICY … WITH (STATE = ON)` |
| RLS blocks admin queries | Missing `IS_MEMBER('db_owner')` bypass | Add to predicate function |
| View returns 0 rows | Gold table empty | Run Scenarios 02 + 04 first |

## Challenge Extension

- Add a `vw_kpi_dashboard` view that pre-aggregates monthly revenue, average order value, and customer count — optimised for a direct-query Power BI dataset.
- Implement column-level security to hide `email_hash` from `analyst-east`.

## Acceptance Criteria

- [ ] All three views return rows when queried as workspace admin
- [ ] `analyst-west` sees only WEST-region customers
- [ ] `analyst-east` sees only EAST-region customers
- [ ] Neither analyst can `SELECT * FROM gold.dim_customer` directly (bypassing views is blocked by RLS)
