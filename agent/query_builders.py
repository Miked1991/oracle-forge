"""
Per-engine query builder modes (Phase 5): single-table, multi-table, repair.

Used by ``LLMQueryGenerator._generate_steps_per_database`` to shorten prompts and
steer the model based on scoped tables and validation / repair notes.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Literal

BuilderKind = Literal["single_table", "multi_table", "repair"]


def classify_builder_kind(scoped_tables: List[str], fix_notes: List[str]) -> BuilderKind:
    """Choose builder mode: repair takes precedence; else single vs multi by scoped object count."""
    if fix_notes:
        return "repair"
    names = [str(x).strip() for x in (scoped_tables or []) if str(x).strip()]
    if len(names) <= 1:
        return "single_table"
    return "multi_table"


def augment_system_for_builder_kind(base_system: str, kind: BuilderKind) -> str:
    """Append a short mode suffix; keeps base dialect / output-shape rules from the caller."""
    if kind == "repair":
        return (
            base_system
            + " REPAIR mode: follow STRUCTURED_REPAIR / NOTES; fix invalid columns or output shape."
        )
    if kind == "single_table":
        return base_system + " SINGLE_TABLE: one scoped table or collection — prefer one FROM / one target collection."
    return base_system + " MULTI_TABLE: multiple scoped tables — use JOINs or subqueries as needed."


def build_per_engine_user_prompt(
    *,
    kind: BuilderKind,
    question: str,
    contract_json: str,
    engine: str,
    rationale: str,
    hints: Any,
    playbook_summary: str,
    eng_hints: List[str],
    schema_json: str,
    err_block: str,
    yelp_parking_extra: str,
) -> str:
    """
    Build the user message for one engine. Labels are shortened vs the legacy monolithic block
    (Phase 5); behavior-critical pieces (contract, schema slice, repair notes) are unchanged.
    """
    hints_json = json.dumps(hints, ensure_ascii=False) if hints else "{}"
    head_parts: List[str] = [
        f"Q:\n{question}",
        f"CONTRACT:\n{contract_json}",
        f"ENGINE: {engine}",
        f"ROUTE:\n{rationale[:400]}",
        f"HINTS_JSON: {hints_json}",
    ]
    if playbook_summary:
        head_parts.append(f"BENCHMARK_CONTEXT:\n{playbook_summary[:1200]}")
    if eng_hints:
        head_parts.append("DATASET_HINTS:\n" + "\n".join(f"- {h}" for h in eng_hints))
    if yelp_parking_extra:
        head_parts.append(yelp_parking_extra.strip())

    head = "\n".join(head_parts) + "\n"

    if kind == "single_table":
        return (
            head
            + f"SCHEMA:\n{schema_json}\n"
            + err_block
            + "Output one JSON object only: SQL → {\"sql\":\"...\"} ; Mongo → {\"collection\":\"...\",\"pipeline\":[...]}. "
            "Use only listed columns/keys.\n"
        )

    if kind == "repair":
        return (
            head
            + f"SCHEMA:\n{schema_json}\n"
            + err_block
            + "Fix the prior attempt per STRUCTURED_REPAIR / NOTES. "
            "Output one JSON object only with the same shape as above.\n"
        )

    # multi_table — slightly more explicit (joins)
    return (
        head
        + f"LINKED_SCHEMA:\n{schema_json}\n"
        + err_block
        + "Generate one read-only query from CONTRACT + LINKED_SCHEMA only. "
        "OUTPUT_JSON: SQL → {\"sql\":\"...\"} ; Mongo → {\"collection\":\"...\",\"pipeline\":[...]}\n"
    )


def schema_slice_summary(schema_json: str, *, max_chars: int = 256) -> str:
    """Compact fingerprint for logs (not a full prompt echo)."""
    s = schema_json.strip()
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 3] + "..."
