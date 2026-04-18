Redesign the oracle-forge agent architecture on branch `fix/design-architecture` into a schema-first, phase-by-phase system that is testable after each phase.

Context:
- Current architecture loads KB markdown into prompts, routes with an LLM, builds per-engine queries, executes through MCP tools, and merges results in Python. The README says there is no vector RAG, schema metadata is merged by `utils/schema_introspection_tool.py`, KB is assembled by `agent/context_builder.py`, routing is in `agent/llm_reasoner.py`, planning is in `agent/planner.py`, and execution/merge happens through `agent/main.py` and related clients. :contentReference[oaicite:0]{index=0}
- The redesign must assume KB files are generated from real databases, not manually authored.
- The main goal is correctness and observability, not minimal code changes.

Design principles:
1. Schema registry is the source of truth, not markdown KB.
2. KB is generated from the schema registry plus curated advisory docs.
3. Query generation is table-scoped, not full-database scoped.
4. Validation happens before execution.
5. Routing must be schema-aware and fail loudly.
6. Every phase must be independently testable.
7. Every step must emit structured logs for debugging and evaluation.

Target architecture:
A. Canonical schema registry
- Build a machine-readable schema registry from:
  - live introspection of PostgreSQL / MongoDB / SQLite / DuckDB
  - dataset config / db_config.yaml
  - verified join metadata
- Include per dataset:
  - databases
  - tables / collections
  - columns / fields
  - types when available
  - primary keys
  - foreign keys / join hints
  - table intent summary
  - provenance and freshness metadata
- Store as JSON artifacts under a deterministic path, e.g. `artifacts/schema_registry/<dataset>.json`

B. Generated KB
- Replace manually maintained schema docs with generated docs from the schema registry.
- Keep advisory KB sections separately marked as non-authoritative.
- Add clear labels:
  - AUTHORITATIVE: generated from live schema
  - ADVISORY: reasoning hints only
  - REFERENCE: not used for validation
- Context builder must stop treating all KB sections equally.

C. Staged planner
Split the current flow into explicit stages:
1. dataset/environment resolution
2. schema registry load or refresh
3. database routing
4. table selection
5. query generation
6. query validation
7. repair loop (up to 3 attempts, pre-execution only)
8. execution
9. merge
10. answer shaping for evaluator format

D. Table-scoped query builders
- Replace the large query-builder prompt with:
  - single-table builder
  - multi-table builder
  - repair builder
- Each builder receives only:
  - question
  - selected engine
  - selected tables
  - selected columns
  - approved join keys
  - required output contract
- Do not send full KB or full schema bundles.

E. Deterministic validator
- Before execution, validate generated SQL / Mongo pipelines against the schema registry:
  - allowed tables only
  - allowed columns only
  - allowed joins only
  - read-only safety
  - syntax parseable
  - output shape compatible with the question
- If validation fails, do not execute; enter a repair loop with structured error messages.

F. Logging and observability
Create structured JSONL logs for every step:
- `logs/schema_registry.jsonl`
- `logs/routing.jsonl`
- `logs/table_selection.jsonl`
- `logs/query_builder.jsonl`
- `logs/query_validation.jsonl`
- `logs/query_repair.jsonl`
- `logs/execution.jsonl`
- `logs/merge.jsonl`
- `logs/eval_trace.jsonl`

Each log entry must include:
- timestamp
- dataset_id
- question
- phase
- selected engine(s)
- selected table(s)
- input artifact references
- output artifact references
- status
- error / warning if any
- attempt number
- duration_ms

G. Evaluator-facing output
- Final agent output must match the benchmark validator expectations exactly:
  - single scalar for scalar questions
  - list of values / rows only when required
- Add a dedicated answer-shaping layer before validation.

Implementation phases (must be individually testable):

Phase 1: Schema registry foundation
- Implement schema registry builders for all supported engines.
- Add tests that assert registry contents for representative datasets.
- Add a command like `python -m scripts.build_schema_registry --dataset yelp`
- Log schema extraction and registry generation.

Phase 2: KB generation
- Generate authoritative KB docs from the registry.
- Separate advisory KB from authoritative KB in the context builder.
- Add tests that ensure generated KB matches registry content.
- Log KB generation inputs and outputs.

Phase 3: Routing redesign
- Make routing consume compact schema summaries derived from the registry.
- Routing output must include selected databases and selected tables.
- Fail fast if routing LLM fails; no heuristic silent fallback.
- Add tests for routing on multiple datasets.
- Log prompts, responses, failures, and normalized routing decisions.

Phase 4: Table selection and scoped schema packaging
- Build a scoped schema package for only the selected tables.
- Add table intent summaries from registry metadata.
- Add tests that ensure only relevant tables are included.
- Log scoped schema payloads.

Phase 5: Query builder split
- Implement:
  - single-table builder
  - multi-table builder
  - repair builder
- Simplify prompts aggressively.
- Add tests with mocked LLM responses and expected query shapes.
- Log all prompts, responses, and selected schema slices.

Phase 6: Deterministic pre-execution validator
- Enforce table/column/join constraints from the registry.
- Support CTEs correctly.
- Support SQL and Mongo validation.
- Add tests for invalid columns, wrong joins, unknown tables, and safe valid queries.
- Log validator decisions and exact rejection reasons.

Phase 7: Repair loop
- On validation failure, retry up to 3 times without executing.
- Feed only scoped schema + validation errors back to the repair builder.
- Add tests proving repair happens before execution.
- Log every repair attempt and final resolution.

Phase 8: Execution and merge cleanup
- Only execute validated steps.
- Improve merge behavior and evaluator-facing answer shaping.
- Add tests for:
  - single-db scalar answers
  - single-db row answers
  - multi-db merged answers
  - empty merge fallback behavior
- Log raw tool outputs, merge strategy, and final shaped answer.

Phase 9: End-to-end evaluation harness integration
- Wire the new pipeline into eval scripts.
- Add debug commands that show:
  - schema snapshot used
  - routing result
  - table selection
  - generated query
  - validation outcome
  - repair attempts
  - execution result
  - normalized actual vs expected
- Add regression tests for previously failing benchmark cases.

Constraints:
- Preserve existing entry points where possible (`run_agent`, eval scripts, CLI, Streamlit), but refactor internals.
- Prefer additive migration with feature flags first, then switch defaults.
- Do not rely on markdown schema docs for hard validation.
- Do not allow silent fallback when authoritative schema is missing.
- Keep prompts short and stage-specific.
- Make every phase runnable and reviewable independently.

Deliverables:
1. A written architecture plan in the repo.
2. The phased implementation.
3. Tests for each phase.
4. Debug/eval commands for each phase.
5. Structured logs for each phase.
6. A migration note explaining how the new architecture replaces the old KB-first prompt architecture.

Before coding:
- inspect the current modules involved in context building, routing, planning, validation, execution, and evaluation
- propose the exact file-by-file change plan
- identify which existing modules can be reused and which should be replaced
- then implement Phase 1 only first, run tests, and stop for review