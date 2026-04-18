# DAB smoke eval trace (4 datasets × 1 query × 1 trial)

**Run command (as executed):**

```text
python eval/run_dab_eval.py --scope multi --per-dataset 1 --trials 1 ^
  --datasets yelp,agnews,PATENTS,stockindex --no-duckdb-enrich --pipeline-debug
```

**Completed:** 2026-04-18 (evaluated_at_utc in `eval/results.json`: `2026-04-18T14:32:21.814580+00:00`)  
**Wall time:** ~28 s (local run)  
**Aggregate:** `pass@1 = 0.0` (0/4 first-trial correct), `overall_trial_accuracy = 0.0`

**Artifacts:**

- `eval/results.json` — full per-query trials + `pipeline_debug` on trial 1  
- `eval/score_log.jsonl` — appended summary line  
- Console capture: `logs/dab_eval_trace_20260418_173157.txt` (if present)

---

## Per-dataset outcomes (what happened)

### 1. `yelp` — *partial pipeline success, wrong benchmark answer*

| Stage | Result |
|--------|--------|
| Routing | Selected `postgresql` |
| Query generation | SQL generated and executed |
| Execution | `postgres_sql_query` **success** (~12 ms) |
| Agent `status` | `success`, `confidence` 1.0 |
| Eval | **`execution_match=False`** |

**Observed SQL (from trace):** filters with `b.description ILIKE '%Indianapolis%'` and `b.state_code = 'IN'`.

**Observed answer:** `average_rating: null` (one row).

**Interpretation:** The **plumbing works** (route → plan → tool → merge). The **SQL does not match the benchmark’s intended semantics** (ground truth expects a different predicate/join pattern), so the numeric result is wrong/null and the validator fails. This is primarily **query semantics / grounding**, not transport or formatting.

---

### 2. `agnews` — *schema gate / wrong dataset scope*

| Stage | Result |
|--------|--------|
| Routing | `predicted_databases` empty in `pipeline_debug` |
| Schema link | Mongo scoped to **`business`, `checkin`** |
| Gate | `need_schema_refresh:empty_collection_fields:business` |
| Execution | **No tool calls** (`used_databases`: []) |

**Interpretation:** Collections **`business` / `checkin` belong to Yelp**, not Ag News. This indicates **dataset/schema context bleed** (registry or routing not scoped to `agnews` for this run), so the planner never produces a valid Ag News query. Failure is **before execution** — routing + schema packaging.

---

### 3. `PATENTS` — *routing hard-stop*

| Stage | Result |
|--------|--------|
| Routing | **`llm_routing_failed`** (`semantic_reason`: `llm_routing_failed`) |
| `used_llm` | `false` in pipeline_debug snippet |
| Plan / SQL | None |
| Execution | None |

**Interpretation:** OpenRouter did not return `selected_databases` that intersect the query’s `available_databases`, so **`run_agent` exits early**. No SQL generation or execution. Pure **routing / JSON contract** failure.

---

### 4. `stockindex` — *schema gate (empty DuckDB column metadata)*

| Stage | Result |
|--------|--------|
| Schema link | `duckdb` / `index_trade` scoped |
| Gate | `need_schema_refresh:empty_column_metadata:index_trade` |
| Preexec repair | Exhausted (schema_gate_failed ×4) |
| Execution | **No tool calls** |

**Important confound:** This eval was run with **`--no-duckdb-enrich`**, which disables live DuckDB column introspection (`ORACLE_FORGE_DUCKDB_SCHEMA_ENRICH=false`). That commonly leaves **`fields` empty** on DuckDB tables unless the registry/KB already lists columns — so the **schema gate correctly refuses** to run “unsafe” SQL.  

**Interpretation:** For this smoke run, stockindex failure is largely explained by **disabled DuckDB enrich + missing pre-filled column metadata** in the bundle, not necessarily by bad routing logic. Re-run **without** `--no-duckdb-enrich` for a fair stockindex test (accept possible longer startup if `DUCKDB_PATH` is slow), or ensure **`index_trade` columns are present in the authoritative registry** when enrich is off.

---

## What is working as expected

1. **Yelp path:** End-to-end agent pipeline (routing → LLM/heuristic SQL → Postgres → merge) completes with `status: success` when Postgres answers.
2. **Eval harness:** Loads four datasets, runs one trial each, writes **`eval/results.json`** and summary JSON; **`pipeline_debug`** captures routing, schema link, repair, and execution summaries.
3. **Failure modes are surfaced:** Routing failure (PATENTS), schema gate (agnews/stockindex), wrong-but-executable SQL (yelp) are all visible in `results.json`.

---

## What is failing (narrowed)

| Bucket | Datasets | Nature |
|--------|----------|--------|
| **Routing** | PATENTS | No valid `selected_databases` vs benchmark list |
| **Schema / dataset isolation** | agnews | Yelp collection names in Ag News scope |
| **Schema gate + DuckDB metadata** | stockindex | `empty_column_metadata` (worse with `--no-duckdb-enrich`) |
| **SQL semantics vs benchmark** | yelp | Executes but wrong filter/join → null / mismatch |

Output formatting / merge is **not** the primary issue in these four rows: three never reach execution; one executes but with the wrong answer.

---

## Suggestions to improve

1. **Routing (PATENTS):** Add **post-parse normalization** (aliases, lowercase) and **fallback** when the LLM returns empty or non-overlapping DBs (e.g. default to `available_databases[:1]` with logged warning), or **constrain** the router JSON schema so the model must pick from the provided list only.

2. **Dataset isolation (agnews):** Ensure **`dataset_id`** flows into registry / `ContextBuilder` / scoped schema so Mongo **never** lists another dataset’s collections. Add a **sanity check**: if scoped collection names are not subsets of that dataset’s `db_config`/registry, reject and refresh.

3. **DuckDB column metadata (stockindex):** Either allow **DuckDB enrich** in eval when testing DuckDB-heavy benchmarks, or **materialize column lists** in the schema registry for `index_trade` so the gate passes with enrich off.

4. **Yelp accuracy:** Add **benchmark-aware hints** or **template checks** for common DAB Yelp queries (e.g. Indianapolis uses `city`/`state` fields per true schema, not only `description`), or **execution-time repair** when `AVG` is null but rows exist.

5. **Eval hygiene:** For multi-dataset smoke tests, document that **`--no-duckdb-enrich` trades correctness on DuckDB** for speed; use it for hang diagnosis, not final scores.

---

## Re-run (fairer stockindex, may be slower)

```powershell
python eval/run_dab_eval.py --scope multi --per-dataset 1 --trials 1 `
  --datasets yelp,agnews,PATENTS,stockindex --pipeline-debug
```

If DuckDB connect stalls, keep timeout env vars (`ORACLE_FORGE_DUCKDB_ENRICH_TIMEOUT_SEC`) instead of disabling enrich entirely.
