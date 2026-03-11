/* ============================================================
   SQL ANALYTICS LIBRARY
   FILE:    cte_patterns_reference.sql
   PURPOSE: Common Table Expression (CTE) patterns for
            readable, maintainable, and reusable analytics.
   
   CTEs COVERED:
   1. Simple CTE          — replacing nested subqueries
   2. Multi-step CTE      — step-by-step transformations
   3. Recursive CTE       — hierarchical/date series data
   4. CTE for deduplication
   5. CTE for pivot-style aggregation
   6. Chained CTEs        — full analytics pipeline pattern
   ============================================================ */


-- ════════════════════════════════════════════════════════════
-- 1. SIMPLE CTE — Replace a messy nested subquery
-- ════════════════════════════════════════════════════════════
-- BAD: nested subquery — hard to read, hard to debug
SELECT *
FROM (
    SELECT customer_id, SUM(net_revenue) AS total
    FROM sales_transactions
    GROUP BY customer_id
) t
WHERE t.total > 10000;

-- GOOD: CTE — readable, easy to modify
WITH high_value_customers AS (
    SELECT
        customer_id,
        SUM(net_revenue)    AS total_revenue
    FROM sales_transactions
    GROUP BY customer_id
)
SELECT *
FROM high_value_customers
WHERE total_revenue > 10000
ORDER BY total_revenue DESC;


-- ════════════════════════════════════════════════════════════
-- 2. MULTI-STEP CTE — Step-by-step transformation pipeline
-- ════════════════════════════════════════════════════════════
-- Business use: complex transformations become readable stages.
-- Each CTE builds on the previous one, like a pipeline.

WITH

-- Step 1: Clean the raw data
cleaned_transactions AS (
    SELECT
        transaction_id,
        UPPER(TRIM(customer_id))                AS customer_id,
        UPPER(TRIM(product_category))           AS product_category,
        CAST(transaction_date AS DATE)          AS transaction_date,
        ROUND(ABS(net_revenue), 2)              AS net_revenue   -- ensure positive
    FROM sales_transactions
    WHERE transaction_id   IS NOT NULL
      AND net_revenue       IS NOT NULL
      AND transaction_date  IS NOT NULL
),

-- Step 2: Aggregate to customer-month level
customer_monthly AS (
    SELECT
        customer_id,
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        SUM(net_revenue)                        AS monthly_revenue,
        COUNT(*)                                AS transaction_count
    FROM cleaned_transactions
    GROUP BY
        customer_id,
        FORMAT(transaction_date, 'yyyy-MM')
),

-- Step 3: Calculate each customer's average monthly spend
customer_averages AS (
    SELECT
        customer_id,
        AVG(monthly_revenue)                    AS avg_monthly_revenue,
        MAX(monthly_revenue)                    AS peak_monthly_revenue,
        COUNT(DISTINCT period)                  AS active_months
    FROM customer_monthly
    GROUP BY customer_id
),

-- Step 4: Join back to flag months significantly above average
flagged_months AS (
    SELECT
        m.customer_id,
        m.period,
        m.monthly_revenue,
        a.avg_monthly_revenue,
        ROUND(m.monthly_revenue / NULLIF(a.avg_monthly_revenue, 0), 2)
                                                AS multiple_of_average,
        CASE
            WHEN m.monthly_revenue > a.avg_monthly_revenue * 2
                THEN 'SPIKE — >2x average'
            WHEN m.monthly_revenue < a.avg_monthly_revenue * 0.5
                THEN 'DIP — <50% of average'
            ELSE 'NORMAL'
        END                                     AS monthly_flag
    FROM customer_monthly       m
    JOIN customer_averages      a ON m.customer_id = a.customer_id
)

-- Final output
SELECT *
FROM flagged_months
WHERE monthly_flag != 'NORMAL'
ORDER BY multiple_of_average DESC;


-- ════════════════════════════════════════════════════════════
-- 3. RECURSIVE CTE — Generate a date series
-- ════════════════════════════════════════════════════════════
-- Business use: fill gaps in time series data. When a period
-- has no transactions, it simply won't appear — but for
-- reporting you often need a zero row for every period.

WITH date_series AS (
    -- Anchor: start date
    SELECT CAST('2024-01-01' AS DATE) AS dt

    UNION ALL

    -- Recursive: add one day until end date
    SELECT DATEADD(day, 1, dt)
    FROM date_series
    WHERE dt < '2024-12-31'
),

-- Get actual daily revenue
daily_revenue AS (
    SELECT
        CAST(transaction_date AS DATE) AS dt,
        SUM(net_revenue)               AS revenue
    FROM sales_transactions
    GROUP BY CAST(transaction_date AS DATE)
)

-- LEFT JOIN ensures every day appears, even with no revenue
SELECT
    d.dt                                AS date,
    COALESCE(r.revenue, 0)             AS revenue,
    CASE WHEN r.revenue IS NULL THEN 'NO TRANSACTIONS' ELSE 'HAS TRANSACTIONS'
    END                                 AS day_status
FROM date_series        d
LEFT JOIN daily_revenue r ON d.dt = r.dt
ORDER BY d.dt
OPTION (MAXRECURSION 400);   -- allow up to 400 iterations (days)


-- ════════════════════════════════════════════════════════════
-- 4. CTE FOR DEDUPLICATION — Safe pattern for any dataset
-- ════════════════════════════════════════════════════════════
-- The safest and most readable way to deduplicate in SQL.
-- Always document WHY you're keeping the row you're keeping.

WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id             -- define "duplicate" key
            ORDER BY
                last_modified_date  DESC,           -- prefer most recently modified
                source_system       ASC             -- if tie, prefer system A over B
        ) AS row_num
    FROM sales_transactions
)
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    net_revenue
    -- Add all columns you need here
FROM deduped
WHERE row_num = 1;


-- ════════════════════════════════════════════════════════════
-- 5. CTE FOR PIVOT-STYLE AGGREGATION (without PIVOT keyword)
-- ════════════════════════════════════════════════════════════
-- Works in all SQL dialects. Produces one column per category
-- without using the PIVOT syntax (which isn't universal).

WITH base AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        product_category,
        net_revenue
    FROM sales_transactions
)
SELECT
    period,
    SUM(CASE WHEN product_category = 'Electronics'  THEN net_revenue ELSE 0 END) AS electronics,
    SUM(CASE WHEN product_category = 'Clothing'     THEN net_revenue ELSE 0 END) AS clothing,
    SUM(CASE WHEN product_category = 'Food'         THEN net_revenue ELSE 0 END) AS food,
    SUM(CASE WHEN product_category = 'Services'     THEN net_revenue ELSE 0 END) AS services,
    SUM(net_revenue)                                                               AS total_revenue
FROM base
GROUP BY period
ORDER BY period;


-- ════════════════════════════════════════════════════════════
-- 6. CHAINED CTEs — Full end-to-end analytics pipeline
-- ════════════════════════════════════════════════════════════
-- Business use: the full pattern you'd use for a real
-- management information report. Every stage is named,
-- readable, and individually testable.

WITH

-- 1. Ingest and clean
raw_cleaned AS (
    SELECT
        transaction_id,
        TRIM(customer_id)               AS customer_id,
        TRIM(product_category)          AS product_category,
        CAST(transaction_date AS DATE)  AS transaction_date,
        net_revenue,
        gross_revenue,
        tax_amount
    FROM sales_transactions
    WHERE transaction_id  IS NOT NULL
      AND net_revenue      > 0
),

-- 2. Enrich with customer dimension
enriched AS (
    SELECT
        r.*,
        c.customer_name,
        c.customer_segment,
        c.region
    FROM raw_cleaned         r
    LEFT JOIN dim_customers  c ON r.customer_id = c.customer_id
),

-- 3. Monthly aggregation
monthly_agg AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        customer_segment,
        region,
        product_category,
        COUNT(DISTINCT transaction_id)          AS transactions,
        COUNT(DISTINCT customer_id)             AS unique_customers,
        SUM(net_revenue)                        AS net_revenue,
        SUM(gross_revenue)                      AS gross_revenue,
        SUM(tax_amount)                         AS tax_collected
    FROM enriched
    GROUP BY
        FORMAT(transaction_date, 'yyyy-MM'),
        customer_segment,
        region,
        product_category
),

-- 4. Add period-on-period comparison
with_variance AS (
    SELECT
        *,
        LAG(net_revenue) OVER (
            PARTITION BY customer_segment, region, product_category
            ORDER BY period
        )                                       AS prior_period_revenue,

        net_revenue -
        LAG(net_revenue) OVER (
            PARTITION BY customer_segment, region, product_category
            ORDER BY period
        )                                       AS revenue_variance
    FROM monthly_agg
)

-- 5. Final output — ready for Power BI or executive report
SELECT
    period,
    customer_segment,
    region,
    product_category,
    transactions,
    unique_customers,
    net_revenue,
    gross_revenue,
    tax_collected,
    prior_period_revenue,
    revenue_variance,
    ROUND(
        100.0 * revenue_variance / NULLIF(prior_period_revenue, 0), 2
    )                                           AS revenue_variance_pct
FROM with_variance
ORDER BY period, net_revenue DESC;
