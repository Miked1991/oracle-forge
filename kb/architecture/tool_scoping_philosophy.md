# Tool Scoping: 40+ Tight Tools > 5 Generic Tools

## From Claude Code Source

Claude Code has 40+ tools, each with single responsibility:

- read_file (not "read_anything")
- edit_file (line-range specific)
- grep_search (pattern matching)
- run_test (specific test file)
- git_commit (with message validation)

## For Oracle Forge Agent

DB-specific tools (one per DB type + operation):

- query_postgres(sql_string, database) → returns JSON
- query_mongodb(collection, pipeline) → returns JSON
- query_sqlite(sql_string, database) → returns JSON
- query_duckdb(sql_string, database) → returns JSON

Join tools:

- resolve_join_key(value, source_db, target_db) → transformed_value
- detect_format_mismatch(value1, value2) → mismatch_type

Transformation tools:

- extract_from_text(text, extraction_schema) → structured_data
- apply_sentiment_classifier(text, lexicon) → sentiment_label

Context tools:

- load_kb_document(document_path) → document_content
- search_correction_log(failure_signature) → matching_entries

## Why This Matters for DAB

A generic "query_database" tool fails because:

- MongoDB needs aggregation pipeline, not SQL
- DuckDB analytical queries need different optimization
- Join key transformation is a separate capability

Tool boundaries = agent capability boundaries. Narrow, DB-specific tools = precise execution.

## Injection Test

Q: Why are 40+ tight tools better than 5 generic tools?
A: Narrow tools enforce precise DB-specific capability boundaries. A generic query_database tool can't handle MongoDB aggregation pipelines vs SQL vs DuckDB analytical optimizations.
