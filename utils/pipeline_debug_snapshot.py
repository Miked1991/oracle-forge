"""
Phase 9: single-structure debug view from a ``run_agent`` outcome (eval / CLI / contract).

Keeps ``agent`` independent of ``eval`` packages.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


def pipeline_debug_enabled() -> bool:
    return os.getenv("ORACLE_FORGE_PIPELINE_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def _tool_summaries(trace: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in trace:
        if not isinstance(item, dict):
            continue
        if not item.get("tool_used") and not item.get("raw_query"):
            continue
        out.append(
            {
                "tool_used": item.get("tool_used"),
                "raw_query": item.get("raw_query"),
                "duration_ms": item.get("duration_ms"),
                "success": item.get("success"),
                "failure_type": item.get("failure_type"),
                "result_summary": item.get("result_summary"),
            }
        )
    return out


def extract_pipeline_debug(
    outcome: Dict[str, Any],
    *,
    schema_info: Optional[Dict[str, Any]] = None,
    max_schema_chars: int = 8000,
) -> Dict[str, Any]:
    plan = outcome.get("plan") if isinstance(outcome.get("plan"), dict) else {}
    qp = plan.get("query_pipeline") if isinstance(plan.get("query_pipeline"), dict) else {}
    trace = outcome.get("query_trace") or outcome.get("trace") or []
    trace_list = trace if isinstance(trace, list) else []

    qp_trace = qp.get("trace") if isinstance(qp.get("trace"), list) else []
    table_selection: Dict[str, Any] = {}
    for entry in qp_trace:
        if not isinstance(entry, dict):
            continue
        if entry.get("phase") == "schema_link":
            eng = str(entry.get("engine") or "unknown")
            table_selection[eng] = {"scoped": entry.get("scoped"), "readiness_ok": entry.get("readiness_ok")}
        if entry.get("phase") == "query_build":
            eng = str(entry.get("engine") or "unknown")
            table_selection.setdefault(eng, {})["query_build"] = {
                "builder_kind": entry.get("builder_kind"),
                "attempts_used": entry.get("attempts_used"),
            }

    routing = {
        "llm_model": (outcome.get("architecture_disclosure") or {}).get("llm_model"),
        "llm_used_for_reasoning": (outcome.get("architecture_disclosure") or {}).get("llm_used_for_reasoning"),
        "mcp_tools_used": (outcome.get("architecture_disclosure") or {}).get("mcp_tools_used"),
        "predicted_databases": [x.get("database") for x in outcome.get("predicted_queries") or [] if isinstance(x, dict)],
    }

    schema_snapshot: Optional[str] = None
    if schema_info is not None:
        try:
            schema_snapshot = json.dumps(schema_info, ensure_ascii=False, indent=2)[:max_schema_chars]
        except (TypeError, ValueError):
            schema_snapshot = str(schema_info)[:max_schema_chars]

    preexec = plan.get("preexec_repair_trace") if isinstance(plan.get("preexec_repair_trace"), list) else []
    closed = outcome.get("closed_loop") or {}

    return {
        "routing": routing,
        "table_selection": table_selection,
        "query_pipeline_metadata": qp.get("metadata"),
        "query_pipeline_trace": qp_trace,
        "generated_queries": outcome.get("predicted_queries"),
        "plan_steps_preview": [
            {
                "database": s.get("database"),
                "dialect": s.get("dialect"),
                "sql_or_pipeline": s.get("query_payload", {}).get("sql")
                or s.get("query_payload", {}).get("pipeline"),
            }
            for s in (plan.get("steps") or [])
            if isinstance(s, dict)
        ],
        "validation": {
            "validation_status": outcome.get("validation_status"),
            "semantic_alignment": outcome.get("semantic_alignment"),
        },
        "repair_attempts": {
            "preexec_repair_trace": preexec,
            "preexec_repair_exhausted": plan.get("preexec_repair_exhausted"),
            "closed_loop": closed,
        },
        "execution": {
            "tool_traces": _tool_summaries(trace_list),
            "merge_info": outcome.get("merge_info"),
            "status": outcome.get("status"),
        },
        "context_layers_used": outcome.get("context_layers_used"),
        "schema_info_snapshot": schema_snapshot,
    }
