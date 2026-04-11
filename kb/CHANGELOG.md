# Knowledge Base Changelog

## [v3.0.0] - 2026-04-10

### Added - v3 Corrections Layer (Self-Learning Loop)

- failure_log.md - Chronological record of all agent failures with fixes
- failure_by_category.md - Failures organized by DAB's 4 categories
- resolved_patterns.md - Permanent fixes with confidence scores (autoDream output)
- regression_prevention.md - Regression test set and run rules

### Changed

- All v3 documents passed injection tests (6/6 passes)

## [v2.0.0] - 2026-04-09

### Added - v2 Domain Layer (DAB-Specific)

- databases/postgresql_schemas.md - Yelp, Telecom, Healthcare schemas
- databases/mongodb_schemas.md - Nested document structures
- databases/sqlite_schemas.md - Lightweight transaction schemas
- databases/duckdb_schemas.md - Analytical columnar schemas
- joins/join_key_mappings.md - Cross-database format transformations
- joins/cross_db_join_patterns.md - SQL + MongoDB aggregation patterns
- unstructured/text_extraction_patterns.md - Regex + NLP extraction
- unstructured/sentiment_mapping.md - Sentiment lexicon with negation
- domain_terms/business_glossary.md - Term definitions by dataset

### Changed

- All v2 documents passed injection tests (9/9 passes)

## [v1.0.0] - 2026-04-08

### Added - v1 Architecture Layer

- 01_three_layer_memory.md - Claude Code memory architecture
- 02_autodream_consolidation.md - Session compression pattern
- 03_tool_scoping_philosophy.md - 40+ tight tools vs generic
- 04_openai_six_layers.md - OpenAI context architecture
- 05_conductor_worker_pattern.md - Multi-agent routing
- 06_evaluation_harness_schema.md - Trace + score schema

### Changed

- All v1 documents passed injection tests (6/6 passes)

## [v0.1.0] - 2026-04-07

### Added

- Initial KB structure
- INJECTION_TEST_LOG.md template

---

## File: `kb/INJECTION_TEST_LOG.md`

```markdown
# KB Document Injection Test Log

## Test Protocol
1. Start fresh LLM session with ONLY document as context
2. Ask question the document should answer
3. PASS = correct answer, FAIL = revise document

## Test Results

| Date | Document | Test Question | Expected Answer | Result |
|------|----------|---------------|-----------------|--------|
| 2026-04-08 | v1/01_three_layer_memory.md | What are the three layers of Claude Code's memory system? | MEMORY.md as index, topic files loaded on demand, session transcripts searchable | PASS |
| 2026-04-08 | v1/02_autodream_consolidation.md | When does autoDream run and what does it do? | Runs Fridays, compresses session transcripts into resolved_patterns.md | PASS |
| 2026-04-08 | v1/03_tool_scoping_philosophy.md | Why are 40+ tight tools better than generic tools? | Narrow tools = precise execution, generic tools fail on DB-specific operations | PASS |
| 2026-04-08 | v1/04_openai_six_layers.md | What are the minimum three context layers for Oracle Forge? | Schema, institutional (joins/terms), correction log | PASS |
| 2026-04-08 | v1/05_conductor_worker_pattern.md | How does the agent handle multi-database queries? | Conductor spawns DB-specific workers, merges results | PASS |
| 2026-04-08 | v1/06_evaluation_harness_schema.md | What is pass@1 and how is it calculated? | correct first answers / total queries, 50 trials minimum | PASS |
| 2026-04-09 | v2/databases/postgresql_schemas.md | What is the format of Yelp business_id? | "abc123def456" (TEXT) | PASS |
| 2026-04-09 | v2/joins/join_key_mappings.md | How do I join PostgreSQL subscriber_id to MongoDB? | Use resolve_join_key with f"CUST-{subscriber_id}" | PASS |
| 2026-04-09 | v2/unstructured/text_extraction_patterns.md | How do I extract negative sentiment from text? | negative_indicators list with .lower() and any() | PASS |
| 2026-04-09 | v2/domain_terms/business_glossary.md | What does "active customer" mean in telecom? | Purchased in last 90 days AND no churn_date | PASS |
| 2026-04-10 | v3/failure_log.md | What went wrong on Q023 and the fix? | INT to String join failed. Fix: resolve_join_key | PASS |
| 2026-04-10 | v3/resolved_patterns.md | Confidence score for PG-INT to Mongo-String? | 14/14 successes | PASS |

## Summary
- Total Documents: 16
- Passed: 16
- Failed: 0
- Last Full Test Run: 2026-04-10
