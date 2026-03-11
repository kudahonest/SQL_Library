# Project 4: SQL Analytics Library

A curated library of production-ready SQL patterns with full business context. Built for analysts who want to understand *why* a technique is used, not just *what* it does.

---

## Why This Exists

Most SQL tutorials show you syntax. Few show you how to apply it to real analytical problems. This library bridges that gap — every pattern includes the business scenario it
solves, commented step by step.

These are the patterns I use daily in large-scale analytics engagements across financial services, retail, logistics, and professional services.

---

## Library Contents

### Window Functions (`/window_functions/`)

| Pattern | Business Use |
|---------|-------------|
| `ROW_NUMBER` | Deduplication — keep latest record per key |
| `RANK / DENSE_RANK` | Revenue league tables, top-N products |
| `LAG / LEAD` | Month-on-month and year-on-year variance |
| `SUM OVER` | Running totals, cumulative revenue |
| `AVG OVER` | 3-month and 12-month moving averages |
| `NTILE` | Customer quartile / decile banding |
| `FIRST_VALUE / LAST_VALUE` | Period open and close values |
| `PERCENT_RANK` | Percentile scoring for anomaly prioritisation |

### CTE Patterns (`/cte_patterns/`)

| Pattern | Business Use |
|---------|-------------|
| Simple CTE | Replace unreadable nested subqueries |
| Multi-step CTE | Pipeline: clean → aggregate → flag → output |
| Recursive CTE | Date series generation — fill gaps in time data |
| CTE deduplication | Cleanest, most readable dedup pattern |
| Pivot-style CTE | Category columns without PIVOT syntax |
| Chained CTE | Full analytics pipeline: ingest → enrich → report |

### Deduplication (`/deduplication/`)

| Pattern | Business Use |
|---------|-------------|
| Simple dedup | Keep latest record per transaction ID |
| Tie-breaking dedup | Multi-rule priority ordering |
| Cross-system dedup | Remove duplicates from merged datasets |
| Near-duplicate detection | Flag same customer, similar amount, close dates |
| Cross-system matching | Full OUTER JOIN to identify record gaps |
| Audit trail | Log what was removed and why |

---

## How to Use

Each file is self-contained. Copy the pattern you need, replace the table and column names with your own, and run.

All patterns include:
- A plain-English description of the business problem
- The SQL technique with full inline comments
- Notes on variations and edge cases

---

## Compatibility

Written for **SQL Server (T-SQL) / Databricks SQL**. With minor syntax adjustments, all patterns work in:
- PostgreSQL (`FORMAT` → `TO_CHAR`, `DATEDIFF` → `DATE_PART`)
- BigQuery (`FORMAT` → `FORMAT_DATE`, window syntax identical)
- Snowflake (syntax largely identical to T-SQL)
- Oracle (minor differences in `FETCH FIRST` and date functions)
