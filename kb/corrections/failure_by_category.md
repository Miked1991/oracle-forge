# Failures Organized by DAB's 4 Categories

## Category 1: Multi-Database Routing Failure

**[Q001]** → Agent queried only PostgreSQL when query needed both PG and MongoDB
**Fix:** Conductor parses query for table references. If tables from >1 DB type, spawn multiple workers.

**[Q056]** → Agent sent SQL to MongoDB worker
**Fix:** MongoDB worker validates input is aggregation pipeline, not SQL. Rejects with clear error.

**[Q089]** → Agent attempted to join results in wrong order (Mongo → PG instead of PG → Mongo)
**Fix:** Always join from primary key source (PG) to foreign key target (Mongo). Never reverse.

---

## Category 2: Ill-Formatted Join Key Mismatch

**[Q023]** → INT customer_id vs "CUST-{INT}" string
**Fix:** resolve_join_key transformation in kb/domain/joins/join_key_mappings.md

**[Q067]** → business_id case mismatch: "ABC123" vs "abc123"
**Fix:** Apply .lower() to both sides before comparison

**[Q102]** → Trailing space in MongoDB string
**Fix:** .strip() all MongoDB string fields before join matching

**[Q156]** → Case-sensitive join on business_id
**Fix:** .lower() transformation on both sides

---

## Category 3: Unstructured Text Extraction Failure

**[Q045]** → Returned raw text instead of count
**Fix:** Apply filter THEN count. Never count raw text fields.

**[Q078]** → Missed "not good" as negative sentiment
**Fix:** Expand lexicon to include negation patterns

**[Q091]** → Extracted wrong medication dose (mg vs mcg)
**Fix:** Include units in extraction pattern, validate against known dose ranges

**[Q134]** → Keyword matching without lowercasing
**Fix:** text.lower() before any indicator matching

---

## Category 4: Domain Knowledge Gap

**[Q034]** → Used "churn" as cancelled within 30 days; correct is any churn_date != NULL
**Fix:** Load domain_terms.md before generating churn queries

**[Q067]** → Used calendar quarters instead of fiscal quarters
**Fix:** Add dataset.fiscal_calendar to domain_terms/ for all datasets

**[Q112]** → "Active customer" = row exists; correct = purchased in last 90 days
**Fix:** Active definition per dataset is in domain_terms.md. Load before WHERE clause.

---

## Search Instructions for Agent

When you encounter a failure:

1. Search this file by category
2. Find similar failure pattern
3. Apply documented fix
4. If no match, append to failure_log.md for human reviewmd. Load before WHERE clause generation.
