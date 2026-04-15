# KB Injection Test Log

## Test Protocol

1. Fresh LLM session with ONLY the document as context
2. Ask the document's own Q&A question
3. PASS = answer contains ≥70% expected keywords

## Run History

| Date | Scope | Score | Pass Rate | Result |
|------|-------|-------|-----------|--------|
| 2026-04-12 | Full Suite | 21/21 | 100.0% | PASS |

## Latest Run — 2026-04-12 16:14:30

**Model:** llama-3.1-8b-instant  
**Pass Rate:** 21/21 (100.0%)  
**Path Validation:** all internal `kb/` references valid

| Total | Passed | Failed | Errors |
|-------|--------|--------|--------|
| 21 | 21 | 0 | 0 |

## Per-Document Results

### ✅ architecture/memory.md — 100%

**Q:** What are the three layers of Claude Code's memory system?  
**A:** According to the document, the three layers of Claude Code's memory system are:  1. **Layer 1 - MEMORY.md (index)** 2. **Layer 2 - Topic Files (on-demand)** 3. **Layer 3 - Session Transcripts (searchable)**  
**Matched:** `MEMORY.md`, `topic files`, `session transcripts`  
**Match Rate:** 100%

### ✅ architecture/autodream_consolidation.md — 75%

**Q:** When does autoDream run and what does it do?  
**A:** autoDream runs on Fridays (weekly) or when the correction log exceeds 50 entries. It compresses session transcripts into persistent memory without losing signal.  
**Matched:** `Fridays`, `compresses`, `session transcripts`  
**Missing:** `resolved_patterns`  
**Match Rate:** 75%

### ✅ architecture/tool_scoping_philosophy.md — 100%

**Q:** Why are 40+ tight tools better than 5 generic tools?  
**A:** Narrow tools enforce precise DB-specific capability boundaries. A generic query_database tool can't handle MongoDB aggregation pipelines vs SQL vs DuckDB analytical optimizations.  
**Matched:** `narrow`, `precise`, `DB-specific`, `boundaries`  
**Match Rate:** 100%

### ✅ architecture/openai_layers.md — 100%

**Q:** What are the minimum three context layers for Oracle Forge?  
**A:** According to the document, the minimum three context layers for Oracle Forge are:  **Layer A (Schema):** Load once per DB type used in session **Layer B (Institutional):** kb/domain/joins + terms (always loaded) **Layer C (Correction / correction log):** kb/corrections/failure_log.md (always loaded)  This is stated in the section "Minimum for Oracle Forge (3 layers that demonstrably work)".  
**Matched:** `Schema`, `institutional`, `correction log`  
**Match Rate:** 100%

### ✅ architecture/conductor_worker_pattern.md — 100%

**Q:** How does the agent handle multi-database queries?  
**A:** The Conductor spawns database-specific workers (one per DB), then merges results from all workers into a unified response.  
**Matched:** `Conductor`, `spawns`, `workers`, `merges`  
**Match Rate:** 100%

### ✅ architecture/evaluation_harness_schema.md — 100%

**Q:** What is pass@1 and how is it calculated?  
**A:** According to the document, pass@1 is calculated as follows:  pass@1 = (correct first answers) / (total queries)  Additionally, the document mentions that the minimum number of trials per query is 50.  
**Matched:** `correct first answers`, `queries`, `minimum`  
**Match Rate:** 100%

### ✅ domain/databases/postgresql_schemas.md — 100%

**Q:** What is the format of Yelp business_id?  
**A:** The format of Yelp business_id is "abc123def456".  
**Matched:** `abc123def456`  
**Match Rate:** 100%

### ✅ domain/databases/mongodb_schemas.md — 100%

**Q:** What is the format of customer_id in MongoDB telecom collection?  
**A:** According to the document, the format of customer_id in MongoDB telecom collection is "CUST-1234567" (STRING with prefix).  
**Matched:** `CUST-`, `STRING`, `prefix`  
**Match Rate:** 100%

### ✅ domain/databases/sqlite_schemas.md — 100%

**Q:** What format are customer_ids in SQLite?  
**A:** According to the document, customer_ids in SQLite are in the format: INTEGER, no prefix.  
**Matched:** `INTEGER`, `no prefix`  
**Match Rate:** 100%

### ✅ domain/databases/duckdb_schemas.md — 100%

**Q:** What is DuckDB used for in DAB?  
**A:** DuckDB is used for analytical queries that aggregate across large datasets.  
**Matched:** `analytical`, `aggregate`, `large datasets`  
**Match Rate:** 100%

### ✅ domain/joins/join_key_mappings.md — 100%

**Q:** How do I join PostgreSQL subscriber_id to MongoDB?  
**A:** Use `resolve_join_key` to apply the transformation `f"CUST-{subscriber_id}"` when joining PostgreSQL `subscriber_id` to MongoDB.  
**Matched:** `resolve_join_key`, `CUST-`, `transformation`  
**Match Rate:** 100%

### ✅ domain/joins/cross_db_join_patterns.md — 75%

**Q:** What are the steps for PostgreSQL to MongoDB join?  
**A:** According to the document, the steps for PostgreSQL to MongoDB join are:  1. Query PostgreSQL first for customer_ids. 2. Transform each ID: f"CUST-{customer_id}" 3. Query MongoDB with transformed IDs 4. Merge results on transformed key  
**Matched:** `transform`, `query Mongo`, `merge`  
**Missing:** `Query PG`  
**Match Rate:** 75%

### ✅ domain/unstructured/text_extraction_patterns.md — 100%

**Q:** How do I extract negative sentiment from support ticket text?  
**A:** According to the document, you can extract negative sentiment from support ticket text by using the `negative_indicators` list with `.lower()` and `any()`.  Here's the code snippet from the document:  ```python negative_indicators = [     'frustrated', 'angry', 'terrible', 'awful', 'worst',     'broken', 'not working', 'failed', 'error', 'complaint',     'unhappy', 'disappointed', 'useless', 'waste', 'terrible' ]  def extract_sentiment(text):     text_lower = text.lower()     is_negative = any(indicator in text_lower for indicator in negative_indicators)     # Check for negation     if 'not ' + indicator in text_lower:         is_negative = False     return 'negative' if is_negative else 'non-negative' ```  You can call the `extract_sentiment` function with the support ticket text as an argument to get the sentiment.  
**Matched:** `negative_indicators`, `.lower()`, `any()`  
**Match Rate:** 100%

### ✅ domain/unstructured/sentiment_mapping.md — 100%

**Q:** How does negation affect sentiment classification?  
**A:** According to the document, negation affects sentiment classification as follows:  - If "not" precedes a negative indicator, the sentiment is still negative. - If "not" precedes a positive indicator, the sentiment is non-negative (not positive). - If "not" precedes a negative indicator that is already in the negative list, the sentiment is still negative.  Additionally, the document provides an example of how negation affects sentiment classification:  - "not good" is negative - "not bad" is non-negative  This is demonstrated in the "Negation Handling" section of the document.  
**Matched:** `not good`, `negative`, `not bad`, `non-negative`  
**Match Rate:** 100%

### ✅ domain/domain_terms/business_glossary.md — 100%

**Q:** What does 'active customer' mean in telecom?  
**A:** According to the document, "active customer" in telecom means: Purchased in last 90 days AND churn_date IS NULL.  
**Matched:** `last 90 days`, `churn_date IS NULL`  
**Match Rate:** 100%

### ✅ corrections/failure_log.md — 100%

**Q:** What went wrong on Q023 and what's the fix?  
**A:** On Q023, the agent attempted to join PostgreSQL subscriber_id (INT) directly with MongoDB collection (String format "CUST-{id}"). The fix is to use resolve_join_key(subscriber_id, 'postgres', 'mongodb') → returns 'CUST-{id}'.  
**Matched:** `INT`, `resolve_join_key`  
**Match Rate:** 100%

### ✅ corrections/failure_by_category.md — 100%

**Q:** What are DAB's four failure categories?  
**A:** DAB's 4 categories are:  1. Multi-Database Routing Failure 2. Ill-Formatted Join Key Mismatch 3. Unstructured Text Extraction Failure 4. Domain Knowledge Gap  
**Matched:** `Multi-Database`, `Join Key`, `Unstructured`, `Domain Knowledge`  
**Match Rate:** 100%

### ✅ corrections/resolved_patterns.md — 100%

**Q:** What is the confidence score for PG-INT to Mongo-String transformation?  
**A:** The confidence score for the PG-INT to Mongo-String transformation is 14/14 successes.  
**Matched:** `14/14`, `successes`  
**Match Rate:** 100%

### ✅ corrections/regression_prevention.md — 100%

**Q:** What happens if regression test fails?  
**A:** If regression test fails on ANY query, the following steps are taken:  1. Revert the change immediately 2. Log failure to failure_log.md 3. Do not deploy until all regression tests pass 4. Update this file with new failure mode  
**Matched:** `Revert`, `log failure`, `do not deploy`  
**Match Rate:** 100%

### ✅ evaluation/dab_scoring_method.md — 100%

**Q:** What is pass@1?  
**A:** pass@1 = (number of correct first answers) / (total queries)  
**Matched:** `correct first answers`, `total queries`  
**Match Rate:** 100%

### ✅ evaluation/submission_format.md — 100%

**Q:** What files are required for DAB submission?  
**A:** According to the document, the files required for DAB submission are:  1. `submission/team_name_results.json` — the results JSON file 2. `AGENT.md` (architecture description)  
**Matched:** `results JSON`, `AGENT.md`  
**Match Rate:** 100%
