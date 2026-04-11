# Agent Failure Log - Chronological

**Format:** [Query ID] → [What Was Wrong] → [Correct Approach]

---

## 2026-04-08

**[Q023]** → INT to String join failure: Agent attempted to join PostgreSQL subscriber_id (INT) directly with MongoDB collection (String format "CUST-{id}")
**Correct:** Use resolve_join_key(subscriber_id, 'postgres', 'mongodb') → returns 'CUST-{id}'

**[Q045]** → Agent returned raw review.text when query asked for "number of reviews mentioning parking"
**Correct:** Extract with contains('parking') filter before counting

**[Q067]** → Agent used calendar Q3 (Jul-Sep) when telecom dataset uses fiscal Q3 (Oct-Dec)
**Correct:** Check dataset.fiscal_calendar from kb/v2-domain/domain_terms/ before applying date filters

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

## Instructions for Agent

1. Read this entire file at session start
2. When you encounter a failure, append to this file
3. Use format exactly: [ID] → [wrong] → [correct]
4. autoDream will consolidate resolved patterns weekly

## Injection Test

Q: What went wrong on Q023 and the fix?
A: INT to String join failed. Fix: resolve_join_key
