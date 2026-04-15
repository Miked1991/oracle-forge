# Agent Failure Log - Chronological

**Format:** [Query ID] → [What Was Wrong] → [Correct Approach]

---

## 2026-04-08

**[Q023]** → INT to String join failure: Agent attempted to join PostgreSQL subscriber_id (INT) directly with MongoDB collection (String format "CUST-{id}")
**Correct:** Use `JoinKeyResolver().resolve_cross_db_join(subscriber_id, mongo_key, 'postgresql', 'mongodb')` from `utils/join_key_resolver.py`. For Telecom: `f"CUST-{subscriber_id}"`. For Healthcare: `f"PT-{patient_id}"`.

```python
from utils.join_key_resolver import JoinKeyResolver
resolver = JoinKeyResolver()
pg_id, mongo_id = resolver.resolve_cross_db_join(
    subscriber_id, mongo_key, 'postgresql', 'mongodb'
)
```

**[Q045]** → Agent returned raw review.text when query asked for "number of reviews mentioning parking"
**Correct:** Extract with contains('parking') filter before counting

**[Q067]** → Agent used calendar Q3 (Jul-Sep) when telecom dataset uses fiscal Q3 (Oct-Dec)
**Correct:** Check dataset.fiscal_calendar from kb/domain/domain_terms/ before applying date filters

---

## 2026-04-09

**[Q089]** → Agent attempted SQL aggregation pipeline on MongoDB
**Correct:** MongoDB worker uses aggregation pipeline: db.collection.aggregate([{$match: {...}}])

**[Q102]** → Agent returned empty result because customer_id format 'CUST-12345' vs 'CUST-12345 ' (trailing space)
**Correct:** Apply .strip() to all MongoDB string keys before matching

**[Q118]** → Agent timed out on 54-query benchmark run
**Correct:** Implement rate limiting: max 10 concurrent workers, 30-second timeout per query

---

## 2026-04-10

**[Q134]** → Agent failed to extract sentiment because used keyword matching without lowercasing
**Correct:** text.lower() before indicator matching

**[Q156]** → Agent attempted to join on business_id without handling case mismatch ('ABC123' vs 'abc123')
**Correct:** Apply .lower() to both sides before comparison

---

## 2026-04-13 — Probe Resolutions

**[J1]** → JOIN on customer_id (PostgreSQL INT) vs customer_ref ("C{id}" STRING in DuckDB) returned 0 rows
**Correct:** Strip "C" prefix and cast to int: `df_churn['cid_int'] = df_churn['customer_ref'].str.lstrip('C').astype(int)`, then merge on integer key. See `utils/join_key_resolver.py`.

**[U2]** → Case-sensitive `LIKE '%urgent%'` in PostgreSQL missed "Urgent" and "URGENT" (~15% undercount)
**Correct:** Use `ILIKE '%urgent%'` in PostgreSQL. Python fallback: `str.contains('urgent', case=False, na=False)`.

**[D3]** → Agent computed intraday return (`close / open - 1`) instead of close-to-close daily return for stock data
**Correct:** Use `df['close'].pct_change()` after sorting by date. First row will be NaN — do NOT fill with 0.

**[D4]** → Agent filtered Yelp businesses on `stars >= 4.5` without checking `is_open`, returning permanently closed businesses
**Correct:** Always include `WHERE is_open = 1 AND stars >= 4.5` when querying active Yelp businesses.

---

## Instructions for Agent

1. Read this entire file at session start
2. When you encounter a failure, append to this file
3. Use format exactly: [ID] → [wrong] → [correct]
4. autoDream will consolidate resolved patterns weekly

## Injection Test

Q: What went wrong on Q023 and the fix?
A: INT to String join failed. Fix: JoinKeyResolver().resolve_cross_db_join(pg_id, mongo_key, 'postgresql', 'mongodb')
