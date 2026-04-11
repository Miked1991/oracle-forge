
---

# V2 - DOMAIN LAYER

## File: `kb/domain/CHANGELOG.md`

```markdown
# v2 Domain Layer Changelog

## [2.0.0] - 2026-04-09

### Added - Databases
- postgresql_schemas.md - Yelp, Telecom, Healthcare schemas with join notes
- mongodb_schemas.md - Nested document structures for all datasets
- sqlite_schemas.md - Lightweight transaction database schemas
- duckdb_schemas.md - Analytical columnar schemas

### Added - Joins
- join_key_mappings.md - Complete transformation mappings across all datasets
- cross_db_join_patterns.md - SQL + MongoDB aggregation pipeline patterns

### Added - Unstructured
- text_extraction_patterns.md - Regex + NLP extraction with code examples
- sentiment_mapping.md - Complete sentiment lexicon with negation handling

### Added - Domain Terms
- business_glossary.md - Definitions for "churn", "active", "fiscal quarter" by dataset

### Injection Test Status
- All 9 documents passed injection tests on 2026-04-09

### Notes
- Each document optimized for direct context injection
- No LLM-pretrained knowledge included
