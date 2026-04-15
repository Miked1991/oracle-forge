# `utils/` — shared runtime helpers

Reusable Python modules used by the Oracle Forge agent and evaluation harness. Import from the package root (`from utils import …`) or from submodules as needed.

| Module | Purpose |
|--------|---------|
| **`query_router.py`** | Implements the Conductor–Worker routing pattern: `route()` to choose database engines, `split_query_for_cross_db()` to emit per-engine `SubQuery` objects, and helpers to build engine-specific query text from schema context. See `kb/architecture/conductor_worker_pattern.md`. |
| **`join_key_resolver.py`** | **`JoinKeyResolver`** — normalizes join keys across systems (prefixes/suffixes, separators, string vs int, case, whitespace, partial matches). Use when linking PostgreSQL numeric IDs to MongoDB string IDs or other mismatched formats. |
| **`schema_introspection_tool.py`** | **`SchemaIntrospectionTool`** — collects schema metadata: prefers MCP `get_schema_metadata()` when available; otherwise parses `DataAgentBench/db_description.txt` for deterministic offline runs. |
| **`schema_introspector.py`** | **`SchemaIntrospector`** — async discovery/cache of table/column metadata across PostgreSQL, SQLite, MongoDB, and DuckDB (parallel fetch, types, samples, relationship hints). |
| **`token_limiter.py`** | **`TokenLimiter`** — rough token estimates (~4 chars/token), prompt truncation, and tool-loop caps for safe prompt sizing and iteration limits. |
| **`rate_limiter.py`** | **`AsyncRateLimiter`** — async token-bucket limiter for LLM API calls (e.g. Groq free tier): `await limiter.acquire()` before each request. |
| **`date_normalizer.py`** | **`DateNormalizer`** — parses varied date strings and normalizes to ISO `YYYY-MM-DD` for consistent filters and cross-database joins; supports fiscal-year and range helpers. |
| **`unstructured_extractor.py`** | **`UnstructuredExtractor`** — regex-first extraction from free text (amounts, dates, emails, product codes, severity, etc.); optional LLM fallback for harder cases. |

## Package exports

`utils/__init__.py` re-exports the main classes for convenience:

`AsyncRateLimiter`, `UnstructuredExtractor`, `JoinKeyResolver`, `DateNormalizer`, `SchemaIntrospector`, `SchemaIntrospectionTool`, `QueryRouter`, `TokenLimiter`.

## Tests

Automated tests live under `tests/utils/` (`test_query_router.py`, `test_rate_limiter.py`, `test_token_limiter.py`, `test_date_normalizer.py`, `test_unstructured_extractor.py`, `test_schema_introspection_tool.py`, `test_schema_introspector.py`). Run: `pytest tests/utils/ -q` from the repo root.

## Dependencies

- **`date_normalizer`** uses `python-dateutil` (see root `requirements.txt`).
- Other modules rely on the standard library plus project types (`MCPToolsClient`, DAB paths) as documented in each file.
