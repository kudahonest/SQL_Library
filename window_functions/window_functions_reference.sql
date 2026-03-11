/* ============================================================
   SQL ANALYTICS LIBRARY
   FILE:    window_functions_reference.sql
   PURPOSE: Reusable window function patterns with business
            context. Copy, adapt, and use in your own work.
   
   WINDOW FUNCTIONS COVERED:
   1.  ROW_NUMBER   — deduplication, ranking
   2.  RANK / DENSE_RANK — league tables, top-N
   3.  LAG / LEAD   — period-on-period comparison
   4.  SUM OVER     — running totals, cumulative revenue
   5.  AVG OVER     — moving averages
   6.  NTILE        — quartile / decile banding
   7.  FIRST_VALUE / LAST_VALUE — period open/close values
   8.  PERCENT_RANK — percentile scoring
   ============================================================ */


-- ════════════════════════════════════════════════════════════
-- 1. ROW_NUMBER — Deduplicate: keep only the latest record
--    per customer
-- ════════════════════════════════════════════════════════════
-- Business use: source systems often have duplicate records.
-- ROW_NUMBER lets you keep exactly one row per key.

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id          -- one group per customer
            ORDER BY transaction_date DESC    -- latest record first
        ) AS rn
    FROM sales_transactions
)
SELECT *
FROM ranked
WHERE rn = 1;   -- keep only the most recent record per customer


-- ════════════════════════════════════════════════════════════
-- 2. RANK / DENSE_RANK — Top 10 products by revenue
-- ════════════════════════════════════════════════════════════
-- RANK     skips numbers after ties (1,1,3,4)
-- DENSE_RANK never skips (1,1,2,3) — better for league tables

SELECT
    product_code,
    product_name,
    SUM(net_revenue)    AS total_revenue,
    RANK() OVER (
        ORDER BY SUM(net_revenue) DESC
    )                   AS revenue_rank,
    DENSE_RANK() OVER (
        ORDER BY SUM(net_revenue) DESC
    )                   AS revenue_dense_rank
FROM sales_transactions
GROUP BY product_code, product_name
ORDER BY revenue_rank
FETCH FIRST 10 ROWS ONLY;   -- top 10 only


-- ════════════════════════════════════════════════════════════
-- 3. LAG / LEAD — Month-on-month revenue variance
-- ════════════════════════════════════════════════════════════
-- Business use: "How did this month compare to last month?"
-- This is one of the most commonly used patterns in MI reporting.

WITH monthly AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        SUM(net_revenue)                        AS revenue
    FROM sales_transactions
    GROUP BY FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    period,
    revenue,

    -- Revenue from prior month
    LAG(revenue, 1) OVER (ORDER BY period)      AS prior_month_revenue,

    -- Absolute variance
    revenue
        - LAG(revenue, 1) OVER (ORDER BY period) AS mom_variance,

    -- Percentage variance
    ROUND(
        100.0 * (revenue - LAG(revenue, 1) OVER (ORDER BY period))
        / NULLIF(LAG(revenue, 1) OVER (ORDER BY period), 0),
    2)                                          AS mom_variance_pct,

    -- Revenue from same month last year (12 months prior)
    LAG(revenue, 12) OVER (ORDER BY period)     AS prior_year_revenue,

    ROUND(
        100.0 * (revenue - LAG(revenue, 12) OVER (ORDER BY period))
        / NULLIF(LAG(revenue, 12) OVER (ORDER BY period), 0),
    2)                                          AS yoy_variance_pct,

    -- Next month's revenue (useful for forecasting context)
    LEAD(revenue, 1) OVER (ORDER BY period)     AS next_month_revenue

FROM monthly
ORDER BY period;


-- ════════════════════════════════════════════════════════════
-- 4. SUM OVER — Running / cumulative revenue total
-- ════════════════════════════════════════════════════════════
-- Business use: "How much have we billed so far this year?"
-- Cumulative totals are essential for budget tracking.

SELECT
    transaction_date,
    transaction_id,
    net_revenue,

    -- Running total across all transactions (ordered by date)
    SUM(net_revenue) OVER (
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS cumulative_revenue,

    -- Running total WITHIN each product category
    SUM(net_revenue) OVER (
        PARTITION BY product_category
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                           AS cumulative_revenue_by_category,

    -- Total for the full dataset (for % of total calculation)
    SUM(net_revenue) OVER ()                    AS grand_total,

    -- Each transaction as % of grand total
    ROUND(
        100.0 * net_revenue / NULLIF(SUM(net_revenue) OVER (), 0), 4
    )                                           AS pct_of_grand_total

FROM sales_transactions
ORDER BY transaction_date;


-- ════════════════════════════════════════════════════════════
-- 5. AVG OVER — 3-month and 12-month moving average
-- ════════════════════════════════════════════════════════════
-- Business use: smooths out spikes to show underlying trend.
-- Used in sales analysis and anomaly detection baseline.

WITH monthly AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        SUM(net_revenue)                        AS revenue
    FROM sales_transactions
    GROUP BY FORMAT(transaction_date, 'yyyy-MM')
)
SELECT
    period,
    revenue,

    -- 3-month rolling average (current + 2 prior months)
    ROUND(AVG(revenue) OVER (
        ORDER BY period
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                       AS rolling_3m_avg,

    -- 12-month rolling average
    ROUND(AVG(revenue) OVER (
        ORDER BY period
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ), 2)                                       AS rolling_12m_avg,

    -- Flag months more than 20% above/below 3m average
    CASE
        WHEN revenue > AVG(revenue) OVER (
                ORDER BY period
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) * 1.2
            THEN 'SPIKE'
        WHEN revenue < AVG(revenue) OVER (
                ORDER BY period
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) * 0.8
            THEN 'DIP'
        ELSE 'NORMAL'
    END                                         AS trend_flag

FROM monthly
ORDER BY period;


-- ════════════════════════════════════════════════════════════
-- 6. NTILE — Band customers into revenue quartiles/deciles
-- ════════════════════════════════════════════════════════════
-- Business use: segment customers for targeted analysis.
-- "Who are our top 25% of customers by spend?"

WITH customer_spend AS (
    SELECT
        customer_id,
        SUM(net_revenue)    AS total_spend,
        COUNT(*)            AS transaction_count
    FROM sales_transactions
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_spend,
    transaction_count,

    -- 4 buckets (quartiles): 1=top 25%, 4=bottom 25%
    NTILE(4) OVER (ORDER BY total_spend DESC)   AS spend_quartile,

    -- 10 buckets (deciles): 1=top 10%, 10=bottom 10%
    NTILE(10) OVER (ORDER BY total_spend DESC)  AS spend_decile,

    -- Readable label
    CASE NTILE(4) OVER (ORDER BY total_spend DESC)
        WHEN 1 THEN 'Tier 1 — Top 25%'
        WHEN 2 THEN 'Tier 2 — Upper Mid'
        WHEN 3 THEN 'Tier 3 — Lower Mid'
        WHEN 4 THEN 'Tier 4 — Bottom 25%'
    END                                         AS customer_tier

FROM customer_spend
ORDER BY total_spend DESC;


-- ════════════════════════════════════════════════════════════
-- 7. FIRST_VALUE / LAST_VALUE — Period open and close values
-- ════════════════════════════════════════════════════════════
-- Business use: what was the opening and closing balance
-- for each customer in a reporting period?

SELECT
    customer_id,
    transaction_date,
    net_revenue,

    -- First transaction value for this customer (opening)
    FIRST_VALUE(net_revenue) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                           AS opening_transaction_value,

    -- Most recent transaction value (closing)
    LAST_VALUE(net_revenue) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                           AS closing_transaction_value,

    -- First transaction date per customer (acquisition date)
    FIRST_VALUE(transaction_date) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                                           AS customer_first_seen

FROM sales_transactions;


-- ════════════════════════════════════════════════════════════
-- 8. PERCENT_RANK — Score every transaction by percentile
-- ════════════════════════════════════════════════════════════
-- Business use: "This transaction is in the top 5% by value."
-- Useful for anomaly prioritisation and risk scoring.

SELECT
    transaction_id,
    customer_id,
    net_revenue,

    ROUND(PERCENT_RANK() OVER (
        ORDER BY net_revenue
    ) * 100, 2)                                 AS revenue_percentile,

    -- Within each product category
    ROUND(PERCENT_RANK() OVER (
        PARTITION BY product_category
        ORDER BY net_revenue
    ) * 100, 2)                                 AS revenue_percentile_in_category,

    -- Label top / bottom 5%
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY net_revenue) >= 0.95
            THEN 'Top 5% — High Value'
        WHEN PERCENT_RANK() OVER (ORDER BY net_revenue) <= 0.05
            THEN 'Bottom 5% — Low Value'
        ELSE 'Mid Range'
    END                                         AS value_band

FROM sales_transactions
ORDER BY revenue_percentile DESC;
