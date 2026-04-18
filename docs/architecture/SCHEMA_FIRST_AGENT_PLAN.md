# Schema-first, phased agent architecture

This document describes the target architecture for oracle-forge, how it replaces the current KB-first prompt flow, and a file-by-file migration map. Implementation proceeds in phases (Phase 1 is schema registry foundation); each phase must ship with tests, structured logs, and debug entry points.

## Pre-implementation inspection summary

The following modules were reviewed as the baseline for context, routing, planning, validation, execution, and evaluation:

- **Context / KB:** `agent/context_builder.py` (layered markdown + merged `schema_metadata`), `utils/schema_bundle.py`, `utils/dataset_playbooks.py`.
- **Schema acquisition:** `utils/schema_introspection_tool.py`, `utils/schema_column_enricher.py`, `utils/schema_readiness.py`, `agent/query_pipeline.py` (partial staging).
- **Routing / planning:** `agent/llm_reasoner.py`, `utils/routing_policy.py`, `agent/planner.py`, `agent/llm_query_generator.py`.
- **Execution / merge / answers:** `agent/main.py`, `agent/tools_client.py`, `agent/query_safety.py`, `utils/question_plan_alignment.py`.
- **Dataset wiring:** `utils/dataset_profiles.py`, `eval/datasets.json`.

**Reuse vs replace** is spelled out in tables below; Phase 1 adds **`utils/schema_registry/`** and **`scripts/build_schema_registry.py`** without changing the live agent path.

## Current state (inspected)

| Concern | Primary modules | Role today |
|--------|------------------|------------|
| KB assembly | `agent/context_builder.py` | Loads layered markdown from `kb/`, merges runtime schema JSON, builds `schema_bundle` + `schema_metadata`. |
| Schema merge / fallback | `utils/schema_introspection_tool.py` | Normalizes MCP schema or parses `DataAgentBench/db_description.txt`. |
| Live column fill | `utils/schema_column_enricher.py` | Introspects SQLite/DuckDB/Postgres/Mongo to fill empty `fields`. |
| Compact LLM schema | `utils/schema_bundle.py` | Builds engine-scoped bundles for query generation. |
| Routing | `agent/llm_reasoner.py`, `utils/routing_policy.py` | OpenRouter routing; compact routing summary from merged metadata. |
| Planning | `agent/planner.py`, `agent/llm_query_generator.py` | DB selection, LLM query steps, schema gate via `utils/schema_readiness.py`. |
| Pipeline phases | `agent/query_pipeline.py` | Answer contract, linked schema payload (partial staging already). |
| Execution / merge | `agent/main.py`, `agent/tools_client.py` | MCP tools, `_merge_outputs`, `_answer_from_metrics`, `_shape_answer_for_eval`. |
| Safety | `agent/query_safety.py` | Payload checks. |
| Dataset env | `utils/dataset_profiles.py`, `eval/datasets.json` | Per-dataset paths and Mongo DB name. |

**Gaps vs target:** Markdown KB is treated as peer to schema; routing lacks explicit table selection; query generation can see large bundles; validation is not fully deterministic pre-execution; logs are not uniform JSONL per phase.

## Target principles

1. **Schema registry** is the only authoritative description of tables, columns, types, keys, and allowed joins (plus explicit provenance).
2. **KB** is generated from the registry + clearly labeled advisory docs (`AUTHORITATIVE` / `ADVISORY` / `REFERENCE`).
3. **Table-scoped** builders receive only selected tables, columns, approved joins, and output contract.
4. **Validate before execute**; repair loop (max 3) uses validator errors + scoped schema only.
5. **Routing** consumes compact registry summaries; failures are loud (no silent heuristic fallback when schema is missing).
6. **Structured JSONL** per phase under `logs/` for debugging and eval.

## Canonical artifact layout

- `artifacts/schema_registry/<dataset_id>.json` — machine-readable registry (Phase 1).
- Optional: `eval/join_metadata/<dataset_id>.json` — verified join graph overrides (merged into registry).
- Logs: `logs/schema_registry.jsonl`, `logs/routing.jsonl`, … (Phase 1 starts with `schema_registry.jsonl`).

## File-by-file migration plan

### Reuse (evolve in place)

| Module | Notes |
|--------|--------|
| `utils/dataset_profiles.py` | Keep as dataset/environment resolution; Phase 1 already uses `load_dataset_profile`. |
| `utils/schema_column_enricher.py` | Logic migrates into registry introspectors; enricher can call registry or shrink to a thin wrapper during migration. |
| `utils/schema_bundle.py` | Becomes a **projection** of the registry (scoped tables only); signatures narrow over time. |
| `utils/schema_readiness.py` | Superseded by deterministic validator (Phase 6); keep until flag flip. |
| `agent/query_pipeline.py` | Align stage names with staged planner; extend dataclasses for validation/repair metadata. |
| `agent/tools_client.py` / MCP | Execution unchanged at the boundary; orchestration tightens (execute only post-validation). |
| `eval/*` | Harness gains debug flags and reads structured logs (Phase 9). |

### Replace or supersede

| Module | Replacement |
|--------|-------------|
| `agent/context_builder.py` (as monolithic KB) | Split: generated authoritative chunks from registry + advisory loaders with explicit labels. |
| `utils/schema_introspection_tool.py` (as source of truth) | **Registry builder** + optional MCP refresh; markdown/`db_description.txt` not used for hard validation. |
| Large monolithic query-builder prompts | `single_table` / `multi_table` / `repair` builders (Phase 5). |
| Ad-hoc merge + answer heuristics in `main.py` | Dedicated merge + **answer shaping** layer (Phase 8) with eval contract tests. |

### New packages / modules (incremental)

| Path | Purpose |
|------|---------|
| `utils/schema_registry/` | Registry model, introspection, build, JSONL logging (Phase 1+). |
| `utils/kb_generation/` (Phase 2) | Render authoritative markdown/JSON from registry. |
| `agent/stages/` or explicit functions in `main.py` | Stages 1–10 with shared log context (Phase 3–8). |
| `utils/query_validation/` (Phase 6) | SQL + Mongo validation against registry. |
| `utils/structured_logs.py` (optional) | Shared JSONL schema for all phases. |

### Feature flags (additive migration)

Suggested env vars (introduce as needed):

- `ORACLE_FORGE_USE_SCHEMA_REGISTRY` — context + routing read registry artifacts.
- `ORACLE_FORGE_STAGED_PIPELINE` — use explicit stage runner vs legacy path.
- Existing: `ORACLE_FORGE_LLM_SQL`, `ORACLE_FORGE_ENRICH_SCHEMA_COLUMNS` — narrow as registry becomes default.

## Phased delivery (testable checkpoints)

| Phase | Deliverable |
|-------|-------------|
| 1 | Registry builders + CLI + `logs/schema_registry.jsonl` + tests. |
| 2 | KB generation from registry; context builder respects AUTHORITATIVE/ADVISORY/REFERENCE. |
| 3 | Schema-aware routing; selected DBs + tables; routing JSONL. |
| 4 | Scoped schema packages; table selection JSONL. |
| 5 | Split query builders + query_builder JSONL. |
| 6 | Deterministic validator + query_validation JSONL. |
| 7 | Repair loop + query_repair JSONL. |
| 8 | Execute validated only; merge + answer shaping; execution/merge JSONL. |
| 9 | Eval harness + debug commands + regression tests. |

## Migration note: KB-first → schema-first

**Before:** Prompts pulled large markdown schema docs and merged MCP/metadata into a single `schema_metadata` blob; enrichment filled gaps at runtime; routing and planning depended on this merged structure.

**After:** A **build step** (or refresh) materializes `artifacts/schema_registry/<dataset>.json` from live introspection + `eval/datasets.json` + optional `eval/join_metadata/<dataset>.json`. The agent loads **that artifact** for validation and compact summaries; markdown is either generated from the registry or clearly marked advisory. If the registry is missing and flags require it, the pipeline **fails loudly** instead of falling back to stale markdown for validation.

**Operator workflow:** Run `python -m scripts.build_schema_registry --dataset <id>` after schema changes or new datasets; commit or CI-cache artifacts as your policy requires.

## Debug / eval commands (rolling)

- Phase 1: `python -m scripts.build_schema_registry --dataset yelp [--output-dir PATH] [--dry-run]`
- Later phases: extend eval CLI / Streamlit to dump stage artifacts (see Phase 9).
