# Agent Architecture: Oracle Forge

## Overview

Oracle Forge is a multi-database analytical agent built on the Conductor-Worker pattern,
designed to answer natural-language queries across PostgreSQL, MongoDB, DuckDB, and SQLite
while resolving cross-engine join key mismatches and unstructured text extraction tasks.
Context is layered in three tiers — per-engine schema, institutional domain knowledge, and
a persistent correction log — with a self-correction loop that consults documented failure
patterns before re-attempting any query that returns suspicious or empty results.

## Key Design Decisions

- **Context layering:**
  Three mandatory layers loaded at session start:
  - Layer A (Schema): `kb/domain/databases/[engine]_schemas.md` — loaded once per engine
    referenced in the session
  - Layer B (Institutional): `kb/domain/joins/join_key_mappings.md` +
    `kb/domain/domain_terms/` — always loaded; covers join keys, business glossary,
    fiscal calendar, and authoritative table registry
  - Layer C (Correction log): `kb/correction/failure_log.md` — always loaded; agent checks
    this before constructing any join that previously produced 0 rows or wrong counts

- **Multi-database routing (Conductor-Worker pattern):**
  The Conductor parses which engines are required, spawns one lightweight worker per engine,
  and each worker receives only its own DB-specific KB subset.  Workers return
  `{result, query_trace, confidence}`; the Conductor merges results in Python using the
  resolved join key from `kb/domain/joins/join_key_mappings.md`.
  Results are **never** merged inside a single SQL query spanning two engines.

- **Self-correction:**
  `kb/correction/failure_log.md` logs every query failure with root cause and fix.
  `kb/correction/resolved_patterns.md` tracks successful transformations with confidence
  scores (e.g. PG-INT → Mongo-String: 14/14).  autoDream consolidates session transcripts
  into these files weekly so institutional memory persists across sessions.

## Mandatory Routing Rules

The following rules are injected into every agent session and must not be overridden
by query-specific instructions.

### Rule 1 — Cross-Engine Scope Check (fixes M1)

> Before planning any query that references **both** customer records **and** support/ticket
> data, call `list_db` on **all** candidate engines.  Do not assume single-DB scope when the
> question mentions customers AND support data simultaneously.

**Enforcement**: The Conductor calls `list_db` across all four engines at session start and
stores the table manifest in working memory before the first tool call.

---

### Rule 2 — Per-Engine Execution Only (fixes M4)

> **NEVER** reference tables from two different database engines in a single SQL statement.
> Always execute one query per engine, collect results as Python lists or DataFrames, then
> merge in Python.

**Correct execution pattern (DuckDB + SQLite example):**
```
(a) DuckDB query  → list_a
(b) SQLite query  → list_b
(c) result = set(list_a) & set(list_b)   # merge in Python, not SQL
```

This rule applies to all four engines.  Cross-engine `JOIN` in raw SQL will always raise
`"no such table"` and must never be attempted.

---

### Rule 3 — PANCANCER_ATLAS / TP53 Dual-Engine Annotation (fixes M5)

> **TP53 and all gene-expression queries always require BOTH DuckDB and PostgreSQL.**
> Never route a PANCANCER_ATLAS question to a single engine.

Join key transformation required:
- DuckDB `gene_expression.patient_id` format: `"TCGA-AB-1234"` (string)
- PostgreSQL `mutations.patient_id` format: `"ab1234"` (alphanumeric, no prefix or dashes)
- Resolution: `resolve_tcga_id(tcga_key)` in `utils/join_key_resolver.py`

---

### Rule 4 — Yelp Rating Source Guard (fixes M2)

> For any query asking for **average rating** or **review score** on Yelp data, always
> recompute from `MongoDB reviews.stars` grouped by `business_id`.
> **Do NOT use `DuckDB business.stars`** — it is a pre-computed aggregate updated weekly
> (stale; documented in `kb/domain/databases/duckdb_schemas.md`).

---

## What Worked

- Three-layer context loading kept critical KB always in scope while avoiding context-window
  overflow; the agent never needed to load all documents at once.
- Conductor-Worker with Python-level merge eliminated every "no such table" cross-engine
  SQL error that appeared in early probe runs (M4 fully resolved after one fix cycle).
- Persistent failure log gave the agent institutional memory: join key transformations
  documented after Q023 (PG-INT → Mongo-String) were correctly applied without re-learning
  on subsequent probes.
- Phrase-level regex patterns for unstructured text (U1 wait-time complaint pattern)
  brought over-counting from 3–4× down to within 5% of ground truth.

## What Didn't

- **Single-DB assumption (M1, M4):** Early sessions silently returned partial results because
  the agent scoped to one engine without checking others.  Fixed by enforcing the `list_db`
  cross-scope check (Rule 1) and the per-engine execution rule (Rule 2).
- **Stale aggregate fields (M2):** `business.stars` in DuckDB looked authoritative but was a
  weekly batch snapshot.  Fixed with a KB guard and system-prompt override directing all
  rating queries to MongoDB `reviews.stars`.
- **Naive LIKE matching (U1):** `WHERE text LIKE '%wait%'` matched positive phrases like
  "can't wait" and "worth the wait", inflating counts 3–4×.  Fixed with a targeted complaint
  regex pattern.
- **Unscoped text classification (U3):** Agent returned raw snippets instead of a classified
  result.  Fixed by enforcing the execute-Python-first rule and adding
  `SentimentClassifier.classify_bulk()` as the mandatory classification step.

## Score

| Category              | Probes | Pass Rate |
|-----------------------|--------|-----------|
| Multi-DB Routing      | 6 / 6  | 100%      |
| Ill-Formatted Join    | 5 / 5  | 100%      |
| Unstructured Text     | 4 / 4  | 100%      |
| Domain Knowledge      | 6 / 6  | 100%      |
| **Overall**           | **21 / 21** | **100%** |

pass@1: ~0.95 (95% CI: [0.91, 0.99])  
Trials: 5 per probe (probe-testing phase); full 50-trial DAB run pending
