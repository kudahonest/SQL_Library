/* ============================================================
   SQL ANALYTICS LIBRARY
   FILE:    deduplication_patterns.sql
   PURPOSE: Production-ready deduplication approaches for
            common analytics scenarios.
   
   SCENARIOS COVERED:
   1. Simple dedup by key — keep latest record
   2. Dedup with tie-breaking rules
   3. Dedup across joined/merged datasets
   4. Fuzzy dedup — near-duplicate detection
   5. Cross-system dedup (multi-source matching)
   6. Audit trail — log what was removed
   ============================================================ */


-- ════════════════════════════════════════════════════════════
-- 1. SIMPLE DEDUP — Keep latest record per key
-- ════════════════════════════════════════════════════════════

WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY last_modified_date DESC
        ) AS rn
    FROM sales_transactions
)
SELECT * FROM deduped WHERE rn = 1;


-- ════════════════════════════════════════════════════════════
-- 2. DEDUP WITH TIE-BREAKING RULES
-- ════════════════════════════════════════════════════════════
-- When two records are identical on all key fields,
-- you need a deterministic rule to break the tie.

WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                customer_id,
                transaction_date,
                net_revenue
            ORDER BY
                -- Rule 1: prefer records with a GL reference (posted)
                CASE WHEN gl_reference IS NOT NULL THEN 0 ELSE 1 END ASC,
                -- Rule 2: prefer records from system A over system B
                CASE WHEN source_system = 'SYSTEM_A'  THEN 0 ELSE 1 END ASC,
                -- Rule 3: if still tied, take the row with the lowest internal ID
                internal_row_id ASC
        ) AS rn
    FROM sales_transactions
)
SELECT * FROM deduped WHERE rn = 1;


-- ════════════════════════════════════════════════════════════
-- 3. DEDUP ACROSS MERGED DATASETS
-- ════════════════════════════════════════════════════════════
-- When combining data from two systems, the same transaction
-- may appear in both. Tag the source and deduplicate.

WITH combined AS (
    SELECT transaction_id, customer_id, net_revenue,
           transaction_date, 'SYSTEM_A' AS source_system
    FROM system_a_transactions

    UNION ALL

    SELECT transaction_id, customer_id, net_revenue,
           transaction_date, 'SYSTEM_B' AS source_system
    FROM system_b_transactions
),
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY
                CASE source_system WHEN 'SYSTEM_A' THEN 1 ELSE 2 END
        ) AS rn,
        COUNT(*) OVER (PARTITION BY transaction_id) AS times_seen
    FROM combined
)
SELECT
    transaction_id,
    customer_id,
    net_revenue,
    transaction_date,
    source_system,
    times_seen,
    CASE WHEN times_seen > 1 THEN 'DUPLICATE FOUND' ELSE 'UNIQUE' END AS dedup_status
FROM deduped
WHERE rn = 1;


-- ════════════════════════════════════════════════════════════
-- 4. NEAR-DUPLICATE DETECTION
-- ════════════════════════════════════════════════════════════
-- Exact dedup won't catch "near duplicates" — same customer,
-- same amount, slightly different date (e.g. re-submitted).
-- Flag transactions that are suspiciously similar.

SELECT
    a.transaction_id                AS txn_1,
    b.transaction_id                AS txn_2,
    a.customer_id,
    a.net_revenue,
    a.transaction_date              AS date_1,
    b.transaction_date              AS date_2,
    ABS(DATEDIFF(day, a.transaction_date, b.transaction_date))
                                    AS days_apart,
    ABS(a.net_revenue - b.net_revenue) AS amount_difference
FROM sales_transactions a
JOIN sales_transactions b
  ON  a.customer_id   = b.customer_id        -- same customer
  AND a.transaction_id < b.transaction_id    -- avoid self-join & double-count
  AND ABS(a.net_revenue - b.net_revenue) < 1 -- within £1
  AND ABS(DATEDIFF(day, a.transaction_date, b.transaction_date)) <= 3 -- within 3 days
ORDER BY days_apart, amount_difference;


-- ════════════════════════════════════════════════════════════
-- 5. CROSS-SYSTEM DEDUP — Full population matching
-- ════════════════════════════════════════════════════════════
-- Used in full-population testing: identify which records
-- appear in source but not in target (and vice versa).

WITH source AS (
    SELECT DISTINCT transaction_id, net_revenue, transaction_date
    FROM source_system
),
target AS (
    SELECT DISTINCT source_reference AS transaction_id, net_amount, posting_date
    FROM gl_postings
)
SELECT
    COALESCE(s.transaction_id, t.transaction_id)    AS transaction_id,
    s.net_revenue                                   AS source_amount,
    t.net_amount                                    AS gl_amount,
    s.transaction_date,
    t.posting_date,
    CASE
        WHEN s.transaction_id IS NULL THEN 'IN_GL_NOT_IN_SOURCE'
        WHEN t.transaction_id IS NULL THEN 'IN_SOURCE_NOT_IN_GL'
        ELSE 'IN_BOTH'
    END                                             AS match_status
FROM source     s
FULL OUTER JOIN target t ON s.transaction_id = t.transaction_id
WHERE s.transaction_id IS NULL
   OR t.transaction_id IS NULL
ORDER BY match_status, transaction_id;


-- ════════════════════════════════════════════════════════════
-- 6. AUDIT TRAIL — Log what was removed during dedup
-- ════════════════════════════════════════════════════════════
-- Best practice: never silently discard duplicates.
-- Always log what was removed and why for audit purposes.

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY last_modified_date DESC
        ) AS rn,
        COUNT(*) OVER (PARTITION BY transaction_id) AS total_copies
    FROM sales_transactions
)
-- Records KEPT
SELECT
    transaction_id,
    customer_id,
    net_revenue,
    'KEPT'                          AS dedup_action,
    total_copies - 1                AS duplicates_removed,
    last_modified_date              AS record_date
FROM ranked
WHERE rn = 1

UNION ALL

-- Records REMOVED (logged for audit)
SELECT
    transaction_id,
    customer_id,
    net_revenue,
    'REMOVED AS DUPLICATE'          AS dedup_action,
    NULL                            AS duplicates_removed,
    last_modified_date              AS record_date
FROM ranked
WHERE rn > 1

ORDER BY transaction_id, dedup_action;
