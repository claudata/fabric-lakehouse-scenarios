-- =============================================================================
-- Fabric Lakehouse SQL Endpoint — Serving Layer Views
-- Run against: lh_advanced_scenarios SQL endpoint
-- Prerequisite: gold.dim_customer and silver.orders are populated
-- =============================================================================
-- OVERVIEW
-- --------
-- This script creates four curated SQL views on the Fabric SQL endpoint, turning
-- Delta tables in OneLake into T-SQL objects directly queryable by Power BI,
-- Excel, client applications, or ad-hoc SQL tools.
--
-- Why views instead of materialised tables?
--   Views execute at query time against the live Delta snapshot, so they always
--   reflect the latest committed data without a separate refresh step. They also
--   let you add computed columns, filter out internal audit fields, and enforce
--   row-level security through predicate injection — none of which requires
--   copying data.
--
-- Architecture notes:
--   • All views target the TWO-LAYER join pattern: silver (transactional facts)
--     joined to gold (conformed SCD2 dimensions). Never join Bronze in serving.
--   • The RLS policy is applied at the base table level (gold.dim_customer);
--     views built on top automatically inherit it — region-restricted users
--     cannot see rows outside their region even through vw_kpi_dashboard.
--   • FORMAT() is used deliberately rather than DATEPART/YEAR+MONTH so the
--     grouping key is a single, readable 'yyyy-MM' string that Power BI can
--     sort lexicographically without an extra date table.
--
-- Execution order:
--   Run all four CREATE OR ALTER VIEW blocks top-to-bottom, then run the
--   validation queries at the bottom to confirm data is flowing correctly.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. Active customer summary by region
-- ---------------------------------------------------------------------------
-- PURPOSE: Provides a quick operational count of active vs. deleted customers
--   per region. Intended for ops dashboards and data steward reviews.
--
-- KEY DESIGN DECISIONS:
--   • Filters WHERE is_current = 1 to read only the current SCD2 row per
--     customer — historical expired rows are excluded so counts are not inflated.
--   • Uses CASE WHEN is_deleted to split active from deleted counts in a single
--     pass, avoiding a second scan or UNION.
--   • MAX(_gold_ts) surfaces the last time the dimension was refreshed for
--     this region, useful for staleness monitoring.
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_customer_summary AS
SELECT
    dc.region,
    COUNT(DISTINCT dc.customer_id)                          AS customer_count,
    SUM(CASE WHEN dc.is_deleted = 0 THEN 1 ELSE 0 END)     AS active_count,
    SUM(CASE WHEN dc.is_deleted = 1 THEN 1 ELSE 0 END)     AS deleted_count,
    MAX(dc._gold_ts)                                        AS last_updated
FROM
    gold.dim_customer dc
WHERE
    dc.is_current = 1
GROUP BY
    dc.region;
GO

-- ---------------------------------------------------------------------------
-- 2. Order summary by status
-- ---------------------------------------------------------------------------
-- PURPOSE: Provides a single-row-per-status breakdown of order volume and value.
--   Acts as the primary OLAP entry point for operations teams tracking
--   fulfilment throughput (e.g. how many orders are still in NEW vs. SHIPPED).
--
-- KEY DESIGN DECISIONS:
--   • Does NOT join to dim_customer — querying the fact table alone makes this
--     view fast and immune to dimension join fanout.
--   • CAST(amount AS DECIMAL(18,2)) guards against Silver rows where amount was
--     coerced to a float during schema drift; explicit decimal arithmetic avoids
--     floating-point summation errors in financial totals.
--   • MIN/MAX order_date per status helps detect anomalies such as orders stuck
--     in NEW for unusually long periods.
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_order_summary AS
SELECT
    o.status,
    COUNT(*)                                AS order_count,
    SUM(CAST(o.amount AS DECIMAL(18, 2)))   AS total_amount,
    AVG(CAST(o.amount AS DECIMAL(18, 2)))   AS avg_amount,
    MIN(o.order_date)                       AS earliest_date,
    MAX(o.order_date)                       AS latest_date
FROM
    silver.orders o
GROUP BY
    o.status;
GO

-- ---------------------------------------------------------------------------
-- 3. Sales by region x month (RLS-enforced via base table policy on dim_customer)
-- ---------------------------------------------------------------------------
-- PURPOSE: Delivers a monthly revenue breakdown by region, suitable for
--   regional managers who should only see their own region's data.
--
-- KEY DESIGN DECISIONS:
--   • Joins silver.orders to gold.dim_customer on email_hash (not customer_id)
--     because customer_id was dropped from Silver as part of PII pseudonymisation
--     in Scenario 02. The join via email_hash is privacy-preserving and still
--     unique per customer.
--   • AND dc.is_current = 1 on the join condition is critical: without it, one
--     order could match multiple SCD2 history rows and appear in multiple regions
--     (if a customer moved region), inflating revenue.
--   • WHERE o.status IN ('SHIPPED', 'NEW') excludes CANCELLED and RETURNED
--     orders so revenue figures represent committed / in-flight value only.
--   • Row-level security on gold.dim_customer is inherited automatically:
--     the SQL engine evaluates the RLS predicate against dc before joining,
--     so a user restricted to region='EMEA' cannot see APAC revenue even here.
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_sales_by_region AS
SELECT
    dc.region,
    FORMAT(o.order_date, 'yyyy-MM')         AS order_month,
    COUNT(o.order_id)                       AS order_count,
    SUM(CAST(o.amount AS DECIMAL(18, 2)))   AS total_revenue,
    AVG(CAST(o.amount AS DECIMAL(18, 2)))   AS avg_order_value
FROM
    silver.orders         o
    INNER JOIN gold.dim_customer dc
        ON  o.customer_id_hash = dc.email_hash   -- join via hashed key
        AND dc.is_current = 1
WHERE
    o.status IN ('SHIPPED', 'NEW')
GROUP BY
    dc.region,
    FORMAT(o.order_date, 'yyyy-MM');
GO

-- ---------------------------------------------------------------------------
-- 4. KPI dashboard view (optimised for direct-query Power BI)
-- ---------------------------------------------------------------------------
-- PURPOSE: Pre-joins and pre-aggregates the two core metrics Power BI needs
--   for a KPI dashboard: revenue per customer and average order value, grouped
--   by region and month. Exposing a single flat view avoids Power BI issuing
--   multiple sub-queries that each re-scan the underlying Delta files.
--
-- KEY DESIGN DECISIONS:
--   • The CTE `monthly` isolates the heavy GROUP BY scan into one named step,
--     making the outer SELECT readable and allowing the optimiser to evaluate
--     the aggregation only once even if the outer query filters further.
--   • NULLIF(total_orders, 0) prevents divide-by-zero when a region+month
--     combination has no matching orders (e.g. a new region onboarded mid-month).
--     ROUND(..., 2) formats the derived metrics to two decimal places before
--     they reach the BI layer, removing display-layer rounding inconsistencies.
--   • Includes RETURNED orders (unlike vw_sales_by_region) so revenue_per_customer
--     captures gross revenue before returns — the distinction matters for
--     gross vs. net margin reporting. Use vw_sales_by_region for net figures.
--   • Suitable for Power BI DirectQuery: the query plan is a single scan +
--     aggregation with no CROSS JOINs or non-equijoins that expand row counts.
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW vw_kpi_dashboard AS
WITH monthly AS (
    SELECT
        dc.region,
        FORMAT(o.order_date, 'yyyy-MM')         AS order_month,
        COUNT(DISTINCT dc.customer_id)           AS active_customers,
        COUNT(o.order_id)                        AS total_orders,
        SUM(CAST(o.amount AS DECIMAL(18, 2)))    AS total_revenue
    FROM
        silver.orders         o
        INNER JOIN gold.dim_customer dc
            ON  o.customer_id_hash = dc.email_hash
            AND dc.is_current = 1
    WHERE
        o.status IN ('SHIPPED', 'NEW', 'RETURNED')
    GROUP BY
        dc.region,
        FORMAT(o.order_date, 'yyyy-MM')
)
SELECT
    region,
    order_month,
    active_customers,
    total_orders,
    total_revenue,
    ROUND(
        total_revenue / NULLIF(total_orders, 0),
        2
    )                                            AS avg_order_value,
    ROUND(
        total_revenue / NULLIF(active_customers, 0),
        2
    )                                            AS revenue_per_customer
FROM
    monthly;
GO

-- ---------------------------------------------------------------------------
-- Validation queries (run after creating views)
-- ---------------------------------------------------------------------------
-- PURPOSE: Quick smoke-tests to verify each view returns data and is correctly
--   joined. Run these in sequence after deploying the views above.
--
-- WHAT TO LOOK FOR:
--   vw_customer_summary — each row should be a region; active_count + deleted_count
--     should equal the total COUNT(DISTINCT customer_id) in silver.customers.
--   vw_order_summary — the SUM of all order_count values should equal
--     SELECT COUNT(*) FROM silver.orders.
--   vw_sales_by_region — check that regions match what is in dim_customer and
--     that no NULL region rows appear (which would indicate a broken join).
--   vw_kpi_dashboard — avg_order_value and revenue_per_customer should both be
--     non-null positive numbers; any NULL indicates a NULLIF guard fired,
--     meaning there are region+month buckets with zero orders or zero customers.
-- ---------------------------------------------------------------------------

-- Check vw_customer_summary
SELECT TOP 10 * FROM vw_customer_summary ORDER BY customer_count DESC;

-- Check vw_order_summary
SELECT * FROM vw_order_summary ORDER BY order_count DESC;

-- Check vw_sales_by_region
SELECT TOP 20 * FROM vw_sales_by_region ORDER BY order_month DESC, total_revenue DESC;

-- Check vw_kpi_dashboard
SELECT TOP 20 * FROM vw_kpi_dashboard ORDER BY order_month DESC, region;
