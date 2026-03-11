/* ============================================================
   SQL ANALYTICS LIBRARY
   FILE:    reconciliation_templates.sql
   PURPOSE: Reusable reconciliation patterns adaptable
            to any two-source or three-source comparison.

   TEMPLATES:
   1. Two-source comparison (generic)
   2. Period-level variance analysis
   3. Running reconciliation status
   4. Multi-dimension reconciliation (by category + period)
   5. Tolerance-based matching
   6. Reconciliation view template (production-ready)
   ============================================================ */


-- ════════════════════════════════════════════════════════════
-- TEMPLATE 1: GENERIC TWO-SOURCE COMPARISON
-- Adapts to ANY two systems by replacing source_a / source_b
-- ════════════════════════════════════════════════════════════

WITH source_a AS (
    -- REPLACE THIS with your first data source
    SELECT
        transaction_id          AS key_field,
        SUM(net_revenue)        AS source_a_amount
    FROM sales_transactions
    GROUP BY transaction_id
),

source_b AS (
    -- REPLACE THIS with your second data source
    SELECT
        source_reference        AS key_field,
        SUM(net_amount)         AS source_b_amount
    FROM gl_postings
    WHERE account_type = 'REVENUE'
    GROUP BY source_reference
)

SELECT
    COALESCE(a.key_field, b.key_field)          AS key_field,
    a.source_a_amount,
    b.source_b_amount,
    COALESCE(a.source_a_amount, 0)
        - COALESCE(b.source_b_amount, 0)        AS variance,
    ABS(COALESCE(a.source_a_amount, 0)
        - COALESCE(b.source_b_amount, 0))       AS abs_variance,

    CASE
        WHEN a.key_field IS NULL                THEN 'IN_B_ONLY'
        WHEN b.key_field IS NULL                THEN 'IN_A_ONLY'
        WHEN ABS(COALESCE(a.source_a_amount,0)
                - COALESCE(b.source_b_amount,0)) <= 0.01
                                                THEN 'MATCHED'
        ELSE                                         'VARIANCE'
    END                                         AS match_status

FROM source_a       a
FULL OUTER JOIN source_b b ON a.key_field = b.key_field
ORDER BY abs_variance DESC;


-- ════════════════════════════════════════════════════════════
-- TEMPLATE 2: PERIOD-LEVEL VARIANCE ANALYSIS
-- Compare totals between two sources by month
-- ════════════════════════════════════════════════════════════

WITH period_source AS (
    SELECT
        FORMAT(transaction_date, 'yyyy-MM')     AS period,
        SUM(net_revenue)                        AS source_total
    FROM sales_transactions
    GROUP BY FORMAT(transaction_date, 'yyyy-MM')
),
period_gl AS (
    SELECT
        FORMAT(posting_date, 'yyyy-MM')         AS period,
        SUM(net_amount)                         AS gl_total
    FROM gl_postings
    WHERE account_type = 'REVENUE'
    GROUP BY FORMAT(posting_date, 'yyyy-MM')
)
SELECT
    COALESCE(s.period, g.period)                AS period,
    ROUND(COALESCE(s.source_total, 0), 2)      AS source_total,
    ROUND(COALESCE(g.gl_total, 0), 2)          AS gl_total,
    ROUND(COALESCE(s.source_total,0)
        - COALESCE(g.gl_total,0), 2)            AS variance,
    ROUND(
        100.0 * (COALESCE(s.source_total,0) - COALESCE(g.gl_total,0))
        / NULLIF(s.source_total, 0), 2
    )                                           AS variance_pct,
    CASE
        WHEN ABS(COALESCE(s.source_total,0)
               - COALESCE(g.gl_total,0)) <= 10   THEN 'GREEN'
        WHEN ABS(COALESCE(s.source_total,0)
               - COALESCE(g.gl_total,0)) <= 1000  THEN 'AMBER'
        ELSE                                          'RED'
    END                                         AS period_rag
FROM period_source  s
FULL OUTER JOIN period_gl g ON s.period = g.period
ORDER BY period;


-- ════════════════════════════════════════════════════════════
-- TEMPLATE 3: RUNNING RECONCILIATION STATUS
-- Cumulative matched vs unmatched as records are processed
-- ════════════════════════════════════════════════════════════

SELECT
    transaction_date,
    transaction_id,
    net_revenue,
    reconciliation_status,

    -- Running count of matched transactions
    SUM(CASE WHEN reconciliation_status = 'MATCHED' THEN 1 ELSE 0 END)
        OVER (ORDER BY transaction_date, transaction_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                AS running_matched_count,

    -- Running total of matched revenue
    SUM(CASE WHEN reconciliation_status = 'MATCHED'
             THEN net_revenue ELSE 0 END)
        OVER (ORDER BY transaction_date, transaction_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                AS running_matched_revenue,

    -- Running reconciliation rate
    ROUND(
        100.0 *
        SUM(CASE WHEN reconciliation_status = 'MATCHED'
                 THEN net_revenue ELSE 0 END)
            OVER (ORDER BY transaction_date, transaction_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        /
        NULLIF(SUM(net_revenue)
            OVER (ORDER BY transaction_date, transaction_id
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0),
    2)                                          AS running_recon_rate_pct

FROM vw_three_way_reconciliation
ORDER BY transaction_date, transaction_id;


-- ════════════════════════════════════════════════════════════
-- TEMPLATE 4: MULTI-DIMENSION RECONCILIATION
-- Break down variances by category AND period simultaneously
-- ════════════════════════════════════════════════════════════

SELECT
    FORMAT(transaction_date, 'yyyy-MM')         AS period,
    product_category,
    COUNT(*)                                    AS transactions,
    SUM(source_net)                             AS source_revenue,
    SUM(CASE WHEN reconciliation_status = 'MATCHED'
             THEN source_net ELSE 0 END)        AS matched_revenue,
    SUM(CASE WHEN reconciliation_status != 'MATCHED'
             THEN ABS(source_vs_gl_variance) ELSE 0 END)
                                                AS variance_amount,
    ROUND(
        100.0 * SUM(CASE WHEN reconciliation_status = 'MATCHED'
                         THEN source_net ELSE 0 END)
        / NULLIF(SUM(source_net), 0), 2
    )                                           AS recon_rate_pct,

    -- Contribution of this category's variance to total period variance
    ROUND(
        100.0 * SUM(CASE WHEN reconciliation_status != 'MATCHED'
                         THEN ABS(source_vs_gl_variance) ELSE 0 END)
        / NULLIF(SUM(SUM(CASE WHEN reconciliation_status != 'MATCHED'
                              THEN ABS(source_vs_gl_variance) ELSE 0 END))
                    OVER (PARTITION BY FORMAT(transaction_date,'yyyy-MM')), 0),
    2)                                          AS pct_of_period_variance

FROM vw_three_way_reconciliation
GROUP BY
    FORMAT(transaction_date, 'yyyy-MM'),
    product_category
ORDER BY period, variance_amount DESC;


-- ════════════════════════════════════════════════════════════
-- TEMPLATE 5: TOLERANCE-BASED MATCHING
-- Match within configurable tolerance thresholds
-- Useful when rounding or currency conversion causes tiny diffs
-- ════════════════════════════════════════════════════════════

DECLARE @tolerance_abs  DECIMAL(10,4) = 0.05;   -- £0.05 absolute
DECLARE @tolerance_pct  DECIMAL(10,4) = 0.001;  -- 0.1% relative

SELECT
    transaction_id,
    source_net,
    gl_net_amount,
    ABS(source_net - COALESCE(gl_net_amount,0))         AS abs_diff,
    ROUND(
        100.0 * ABS(source_net - COALESCE(gl_net_amount,0))
        / NULLIF(source_net, 0), 4
    )                                                   AS pct_diff,

    CASE
        WHEN gl_net_amount IS NULL
            THEN 'MISSING_IN_GL'
        WHEN ABS(source_net - gl_net_amount) <= @tolerance_abs
            THEN 'MATCHED (within £' + CAST(@tolerance_abs AS VARCHAR) + ')'
        WHEN ABS(source_net - gl_net_amount) / NULLIF(source_net,0)
             <= @tolerance_pct
            THEN 'MATCHED (within ' + CAST(@tolerance_pct*100 AS VARCHAR) + '%)'
        ELSE 'VARIANCE'
    END                                                 AS match_status_with_tolerance

FROM vw_three_way_reconciliation
ORDER BY abs_diff DESC;
