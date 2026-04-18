# Migration: KB-first prompts → schema-first registry

This note summarizes how operators and developers should move from the legacy **markdown-heavy knowledge base** model to the **canonical schema registry** model. The full phased plan lives in [SCHEMA_FIRST_AGENT_PLAN.md](SCHEMA_FIRST_AGENT_PLAN.md).

## What changes conceptually

| Before | After |
|--------|--------|
| Schema truth split across `kb/domain/databases/*.md`, MCP metadata, and runtime merge | **`artifacts/schema_registry/<dataset>.json`** is authoritative for structure (tables, columns, keys, allowed joins) |
| `SchemaIntrospectionTool` + `db_description.txt` as fallback | Fallbacks remain for **legacy agent path only**; validation and routing must use the registry once feature flags are on |
| Large undifferentiated context layers | **AUTHORITATIVE** (generated from registry) vs **ADVISORY** / **REFERENCE** (never used for hard validation) |

## Phase 1 (shipped): registry build

1. Configure dataset connections via `eval/datasets.json` and/or `ORACLE_FORGE_DATASET_<ID>_*` env vars (see `utils/dataset_profiles.py`).
2. Set `POSTGRES_DSN`, `MONGODB_URI`, and `MONGODB_DATABASE` in `.env` (or the environment) so **Docker-hosted** Postgres/Mongo are reachable from the host (e.g. `localhost` ports mapped from containers). The builder loads `.env` automatically. Per-dataset Mongo database names in `eval/datasets.json` override a generic `MONGODB_DATABASE` when both are set.
3. Ensure **SQLite** paths in `eval/datasets.json` point at real files (e.g. DAB `query_dataset/*.db`); missing files are skipped.
4. Run one dataset or all:

   `python -m scripts.build_schema_registry --dataset yelp`  
   `python -m scripts.build_schema_registry --all-datasets`

5. Inspect `artifacts/schema_registry/<dataset>.json` and `logs/schema_registry.jsonl`.

Optional verified joins: add `eval/join_metadata/<dataset>.json` with `{ "joins": [ ... ] }` (merged into the registry).

## Safe rollout

New behavior is introduced behind additive modules and future env flags (`ORACLE_FORGE_USE_SCHEMA_REGISTRY`, etc.). Existing `run_agent` and eval entry points stay stable until later phases switch defaults.
