/* ============================================================
   SQL ANALYTICS LIBRARY
   FILE:    string_aggregation_patterns.sql
   PURPOSE: Patterns for aggregating strings across rows —
            one of the most useful but underused SQL skills
            in data analytics and reporting.

   TECHNIQUES COVERED:
   1. STRING_AGG        — modern standard (SQL Server 2017+, PostgreSQL)
   2. LISTAGG           — Oracle / Snowflake
   3. GROUP_CONCAT      — MySQL / SQLite
   4. FOR XML PATH      — legacy SQL Server (pre-2017)
   5. Ordered aggregation
   6. Conditional string aggregation
   7. Pivoting with string agg
   8. Exception reporting patterns
   ============================================================ */


-- ════════════════════════════════════════════════════════════
-- 1. BASIC STRING_AGG
--    Combine multiple rows into one comma-separated string
-- ════════════════════════════════════════════════════════════
-- Business use: "List all products purchased by each customer"

SELECT
    customer_id,
    COUNT(DISTINCT product_code)                AS product_count,
    STRING_AGG(product_code, ', ')
        WITHIN GROUP (ORDER BY product_code)    AS products_purchased,
    STRING_AGG(CAST(net_revenue AS VARCHAR(20)), ' | ')
        WITHIN GROUP (ORDER BY transaction_date) AS revenue_history
FROM sales_transactions
GROUP BY customer_id
ORDER BY product_count DESC;


-- ════════════════════════════════════════════════════════════
-- 2. DISTINCT VALUES ONLY (deduplicate before aggregating)
-- ════════════════════════════════════════════════════════════
-- STRING_AGG itself doesn't support DISTINCT — use a subquery.

SELECT
    customer_id,
    STRING_AGG(product_category, ', ')
        WITHIN GROUP (ORDER BY product_category) AS distinct_categories
FROM (
    SELECT DISTINCT customer_id, product_category
    FROM sales_transactions
) t
GROUP BY customer_id;


-- ════════════════════════════════════════════════════════════
-- 3. EXCEPTION REPORTING — List issues per period
-- ════════════════════════════════════════════════════════════
-- Business use: "What exception types occurred in each month?"
-- Perfect for management information reports.

SELECT
    FORMAT(transaction_date, 'yyyy-MM')         AS period,
    COUNT(CASE WHEN reconciliation_status != 'MATCHED' THEN 1 END)
                                                AS exception_count,
    STRING_AGG(
        CASE WHEN reconciliation_status != 'MATCHED'
             THEN reconciliation_status
        END,
        ', '
    )                                           AS exception_types,
    STRING_AGG(
        CASE WHEN reconciliation_status != 'MATCHED'
             THEN transaction_id + ' (' + reconciliation_status + ')'
        END,
        '; '
    )                                           AS exception_detail
FROM sales_transactions
GROUP BY FORMAT(transaction_date, 'yyyy-MM')
ORDER BY period;


-- ════════════════════════════════════════════════════════════
-- 4. ORDERED AGGREGATION — Build a timeline or history
-- ════════════════════════════════════════════════════════════
-- Business use: "Show the transaction history for each customer
--               in date order as a single readable string"

SELECT
    customer_id,
    STRING_AGG(
        FORMAT(transaction_date, 'dd MMM yyyy') + ': £'
        + FORMAT(net_revenue, 'N2'),
        ' → '
    ) WITHIN GROUP (ORDER BY transaction_date)  AS transaction_timeline
FROM sales_transactions
GROUP BY customer_id
ORDER BY customer_id;


-- ════════════════════════════════════════════════════════════
-- 5. CONDITIONAL STRING AGGREGATION
--    Different strings for different conditions
-- ════════════════════════════════════════════════════════════
-- Business use: produce a human-readable summary per customer

SELECT
    customer_id,
    -- High-value transactions only
    STRING_AGG(
        CASE WHEN net_revenue > 1000
             THEN 'TXN ' + transaction_id + ' [£' + FORMAT(net_revenue,'N2') + ']'
        END,
        ', '
    )                                           AS high_value_transactions,

    -- Problem transactions
    STRING_AGG(
        CASE WHEN reconciliation_status != 'MATCHED'
             THEN reconciliation_status + ': ' + transaction_id
        END,
        ' | '
    )                                           AS exceptions

FROM sales_transactions
GROUP BY customer_id
HAVING COUNT(CASE WHEN net_revenue > 1000 THEN 1 END) > 0
    OR COUNT(CASE WHEN reconciliation_status != 'MATCHED' THEN 1 END) > 0
ORDER BY customer_id;


-- ════════════════════════════════════════════════════════════
-- 6. PIVOT USING STRING_AGG
--    Produce a crosstab-style report without PIVOT syntax
-- ════════════════════════════════════════════════════════════

WITH monthly_by_category AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        product_category,
        SUM(net_revenue)                        AS revenue
    FROM sales_transactions
    GROUP BY FORMAT(transaction_date, 'yyyy-MM'), product_category
)
SELECT
    period,
    -- One column per category showing revenue
    STRING_AGG(
        product_category + ': £' + FORMAT(revenue, 'N0'),
        '  |  '
    ) WITHIN GROUP (ORDER BY product_category) AS revenue_by_category
FROM monthly_by_category
GROUP BY period
ORDER BY period;


-- ════════════════════════════════════════════════════════════
-- 7. LEGACY: FOR XML PATH (SQL Server pre-2017)
--    Same result as STRING_AGG but works on older versions
-- ════════════════════════════════════════════════════════════

SELECT
    t.customer_id,
    STUFF(
        (
            SELECT ', ' + s.product_code
            FROM sales_transactions s
            WHERE s.customer_id = t.customer_id
            ORDER BY s.product_code
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'),
        1, 2, ''                    -- remove leading ', '
    )                               AS products_purchased
FROM sales_transactions t
GROUP BY t.customer_id;


-- ════════════════════════════════════════════════════════════
-- 8. REAL-WORLD PATTERN: Data Quality Exception Report
--    One row per period, listing all exception transaction IDs
--    — exactly the kind of output used in audit reporting
-- ════════════════════════════════════════════════════════════

WITH exceptions AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        reconciliation_status,
        transaction_id,
        ROUND(ABS(source_vs_gl_variance), 2)    AS variance_amount
    FROM vw_three_way_reconciliation
    WHERE reconciliation_status != 'MATCHED'
)
SELECT
    period,
    COUNT(*)                                    AS total_exceptions,
    SUM(variance_amount)                        AS total_variance,

    -- One string listing all exception IDs and their type
    STRING_AGG(
        '[' + transaction_id + ' — ' + reconciliation_status
        + ' £' + CAST(variance_amount AS VARCHAR(20)) + ']',
        CHAR(10)    -- newline separator for readability
    ) WITHIN GROUP (ORDER BY variance_amount DESC)
                                                AS exception_register

FROM exceptions
GROUP BY period
ORDER BY period;
