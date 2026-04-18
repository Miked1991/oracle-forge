"""

Dry-run OpenRouter routing (Phase 3) plus the post-routing scoped schema package (Phase 4).

No MCP, planner, or execution — uses ``artifacts/schema_registry/<dataset>.json`` to hydrate

``schema_metadata`` when ``--dataset`` is set so table selection + scoped bundles match production.



Requires OPENROUTER_API_KEY in the environment or .env.



Examples:

  python -m scripts.routing_probe -q "Average rating by business in Indianapolis" --dataset yelp

  python -m scripts.routing_probe -q "Count articles" --dataset agnews --databases postgresql,mongodb,sqlite,duckdb

  python -m scripts.routing_probe -q "..." --dataset yelp --query-gen

  (Phase 6) JSON includes query_generation.pre_execution_validation and logs/pre_execution_validator.jsonl.

  python -m scripts.routing_probe -q "..." --dataset yelp --plan-only

  (Phases 7–8) ``--plan-only`` runs ``QueryPlanner.create_plan`` (set ORACLE_FORGE_LLM_SQL=true). Logs: logs/preexec_repair.jsonl.

"""



from __future__ import annotations



import argparse

import json

import os

import sys

import time

from pathlib import Path

from typing import Any, Dict



from dotenv import load_dotenv



from agent.llm_reasoner import LLMRoutingFailed, OpenRouterRoutingReasoner

from utils.dataset_playbooks import load_dataset_playbook

from utils.schema_bundle import build_schema_bundle, schema_bundle_json

from utils.schema_registry.routing_compact import load_registry_json_optional

from utils.scoped_schema_pack import rebuild_with_scoped_pack, schema_metadata_stub_from_registry

from utils.token_limiter import TokenLimiter






def _parse_databases(raw: str) -> list[str]:

    return [x.strip().lower() for x in raw.split(",") if x.strip()]





def _bundle_table_names(bundle: Dict[str, Any]) -> Dict[str, Any]:

    out: Dict[str, Any] = {}

    for db, block in (bundle.get("engines") or {}).items():

        if not isinstance(block, dict):

            continue

        out[str(db)] = {

            "tables": [t.get("name") for t in block.get("tables") or [] if isinstance(t, dict)],

            "collections": [c.get("name") for c in block.get("collections") or [] if isinstance(c, dict)],

        }

    return out





def _intent_snippets(bundle: Dict[str, Any], max_tables: int = 8) -> list[Dict[str, Any]]:

    snippets: list[Dict[str, Any]] = []

    n = 0

    for db, block in (bundle.get("engines") or {}).items():

        if not isinstance(block, dict):

            continue

        for t in block.get("tables") or []:

            if not isinstance(t, dict) or n >= max_tables:

                break

            if t.get("intent_summary"):

                snippets.append(

                    {

                        "engine": db,

                        "name": t.get("name"),

                        "intent_summary": str(t["intent_summary"])[:320],

                    }

                )

                n += 1

        for c in block.get("collections") or []:

            if not isinstance(c, dict) or n >= max_tables:

                break

            if c.get("intent_summary"):

                snippets.append(

                    {

                        "engine": db,

                        "name": c.get("name"),

                        "intent_summary": str(c["intent_summary"])[:320],

                    }

                )

                n += 1

    return snippets





def main(argv: list[str] | None = None) -> int:

    parser = argparse.ArgumentParser(

        description="Probe routing + scoped schema pack (registry-hydrated when --dataset is set).",

    )

    parser.add_argument("-q", "--question", required=True, help="Natural-language question to route.")

    parser.add_argument(

        "--dataset",

        default="",

        help="Dataset id: loads artifacts/schema_registry/<dataset>.json for routing + scoped pack.",

    )

    parser.add_argument(

        "--databases",

        default="postgresql,mongodb,sqlite,duckdb",

        help="Comma-separated engines the router may choose (default: all four).",

    )

    parser.add_argument(

        "--no-routing-log",

        action="store_true",

        help="Skip appending logs/routing.jsonl.",

    )

    parser.add_argument(

        "--no-scoped-log",

        action="store_true",

        help="Skip appending logs/scoped_schema.jsonl.",

    )

    parser.add_argument(

        "--json-preview-chars",

        type=int,

        default=6000,

        help="Max characters of schema_bundle_json to include in output (default: 6000).",

    )

    parser.add_argument(

        "--query-gen",

        action="store_true",

        help="After scoped schema pack, run LLMQueryGenerator.generate_steps (needs API keys).",

    )

    parser.add_argument(

        "--no-query-builder-log",

        action="store_true",

        help="Skip appending logs/query_builder.jsonl.",

    )

    parser.add_argument(

        "--no-preexec-validation-log",

        action="store_true",

        help="Skip appending logs/pre_execution_validator.jsonl (Phase 6).",

    )

    parser.add_argument(

        "--plan-only",

        action="store_true",

        help="After scoped pack, run QueryPlanner.create_plan (Phase 7 repair loop; needs ORACLE_FORGE_LLM_SQL=true and API keys).",

    )

    parser.add_argument(

        "--no-preexec-repair-log",

        action="store_true",

        help="Skip appending logs/preexec_repair.jsonl (Phase 7).",

    )

    args = parser.parse_args(argv)



    repo_root = Path(__file__).resolve().parents[1]

    load_dotenv(repo_root / ".env", override=False)

    if args.no_routing_log:

        os.environ["ORACLE_FORGE_DISABLE_ROUTING_LOG"] = "1"

    if args.no_scoped_log:

        os.environ["ORACLE_FORGE_DISABLE_SCOPED_SCHEMA_LOG"] = "1"

    if args.no_query_builder_log:

        os.environ["ORACLE_FORGE_QUERY_BUILDER_LOG"] = "false"

    if args.no_preexec_validation_log:

        os.environ["ORACLE_FORGE_PREEXEC_VALIDATION_LOG"] = "false"

    if args.no_preexec_repair_log:

        os.environ["ORACLE_FORGE_PREEXEC_REPAIR_LOG"] = "false"



    available = _parse_databases(args.databases)

    if not available:

        print("No databases listed.", file=sys.stderr)

        return 2



    dataset_id = (args.dataset or "").strip() or None

    playbook = load_dataset_playbook(dataset_id, repo_root) if dataset_id else None



    registry = load_registry_json_optional(repo_root, dataset_id) if dataset_id else None

    schema_metadata: Dict[str, Any] = schema_metadata_stub_from_registry(registry) if registry else {}



    bundle_pre = build_schema_bundle(

        schema_metadata,

        available,

        dataset_id,

        playbook=playbook if playbook else None,

    )



    context: Dict[str, Any] = {

        "user_question": args.question,

        "routing_question": args.question,

        "dataset_id": dataset_id,

        "schema_metadata": schema_metadata,

        "context_layers": {},

        "schema_bundle_json": schema_bundle_json(bundle_pre),

        "dataset_playbook": playbook,

    }



    reasoner = OpenRouterRoutingReasoner(repo_root=repo_root, token_limiter=TokenLimiter())

    t0 = time.perf_counter()

    try:

        g = reasoner.plan(question=args.question, available_databases=available, context=context)

    except LLMRoutingFailed as exc:

        ms = int((time.perf_counter() - t0) * 1000)

        print(

            json.dumps(

                {

                    "ok": False,

                    "error": str(exc),

                    "duration_ms": ms,

                    "registry_hydrated": bool(registry),

                    "registry_path": str(dataset_id) if dataset_id else None,

                },

                indent=2,

            )

        )

        return 3



    route_ms = int((time.perf_counter() - t0) * 1000)



    context["llm_guidance"] = {

        "selected_databases": g.selected_databases,

        "selected_tables": g.selected_tables,

        "rationale": g.rationale,

        "query_hints": g.query_hints,

        "model": g.model,

        "used_llm": g.used_llm,

    }



    pack_t0 = time.perf_counter()

    rebuild_with_scoped_pack(context, available, dataset_id, repo_root=repo_root)

    pack_ms = int((time.perf_counter() - pack_t0) * 1000)



    sb = context.get("schema_bundle") or {}

    sj = context.get("schema_bundle_json") or ""

    preview = sj[: max(0, args.json_preview_chars)]

    if len(sj) > len(preview):

        preview = preview + "\n... (truncated; full length " + str(len(sj)) + " chars)"



    out: Dict[str, Any] = {

        "ok": True,

        "registry_hydrated": bool(registry),

        "routing": {

            "selected_databases": g.selected_databases,

            "selected_tables": g.selected_tables,

            "rationale": g.rationale,

            "model": g.model,

            "duration_ms": route_ms,

        },

        "scoped_pipeline": {

            "schema_bundle_mode": context.get("schema_bundle_mode"),

            "duration_ms": pack_ms,

            "tables_per_engine": _bundle_table_names(sb),

            "intent_snippets": _intent_snippets(sb),

            "schema_bundle_json_chars": len(sj),

            "schema_bundle_json_preview": preview,

        },

    }



    if args.plan_only:

        from agent.planner import QueryPlanner



        plan_t0 = time.perf_counter()

        planner = QueryPlanner(context)

        plan = planner.create_plan(args.question, available, routing_question=args.question)

        plan_ms = int((time.perf_counter() - plan_t0) * 1000)

        out["plan_probe"] = {

            "duration_ms": plan_ms,

            "plan": plan,

            "preexec_repair_log": str(repo_root / "logs" / "preexec_repair.jsonl"),

            "hint": "Set ORACLE_FORGE_LLM_SQL=true and LLM API keys for Phase 7 pre-exec repair (no tool execution here).",

        }



    if args.query_gen:

        from agent.llm_query_generator import LLMQueryGenerator

        from agent.query_safety import validate_llm_generated_steps



        qg_t0 = time.perf_counter()

        gen = LLMQueryGenerator(repo_root=repo_root)

        steps_out = gen.generate_steps(args.question, g.selected_databases, context)

        qg_ms = int((time.perf_counter() - qg_t0) * 1000)

        qg_block: Dict[str, Any] = {

            "duration_ms": qg_ms,

            "provider": gen.provider,

            "model": gen.model_name,

            "result": steps_out,

            "pre_execution_validation_log": str(repo_root / "logs" / "pre_execution_validator.jsonl"),

        }

        if steps_out is None:

            qg_block["note"] = (

                "generator returned None (set OPENROUTER_API_KEY or GROQ_API_KEY; check LLM_PROVIDER)."

            )

        elif isinstance(steps_out, dict):

            st = steps_out.get("steps")

            if isinstance(st, list) and st:

                pv_ok, pv_errs = validate_llm_generated_steps(

                    st,

                    context.get("schema_metadata") or {},

                )

                qg_block["pre_execution_validation"] = {"all_ok": pv_ok, "errors": pv_errs}

        out["query_generation"] = qg_block

    print(json.dumps(out, ensure_ascii=False, indent=2))

    return 0





if __name__ == "__main__":

    raise SystemExit(main())


