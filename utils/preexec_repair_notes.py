"""Phase 7: compact scoped-schema hints + pre-execution failure lines for LLM repair."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


def _snippet_chars() -> int:
    return max(500, int(os.getenv("ORACLE_FORGE_PREEXEC_SCOPED_SNIPPET_CHARS", "3500")))


def compact_scoped_schema_hint(context: Dict[str, Any]) -> str:
    """
    Prefer table/collection names from ``schema_bundle``; fall back to truncated ``schema_bundle_json``.
    """
    max_c = _snippet_chars()
    bundle = context.get("schema_bundle")
    if isinstance(bundle, dict) and bundle.get("engines"):
        lines: List[str] = []
        for db, eng in list((bundle.get("engines") or {}).items())[:8]:
            if not isinstance(eng, dict):
                continue
            tnames = [
                str(t.get("name"))
                for t in (eng.get("tables") or [])[:16]
                if isinstance(t, dict) and t.get("name")
            ]
            cnames = [
                str(c.get("name"))
                for c in (eng.get("collections") or [])[:12]
                if isinstance(c, dict) and c.get("name")
            ]
            if tnames:
                lines.append(f"{db} tables: {', '.join(tnames)}")
            if cnames:
                lines.append(f"{db} collections: {', '.join(cnames)}")
        hint = "\n".join(lines).strip()
        if hint:
            return hint[:max_c]
    sj = (context.get("schema_bundle_json") or "").strip()
    if sj:
        return sj[:max_c]
    return ""


def build_preexec_failure_notes(
    gen_out: Optional[Dict[str, Any]],
    context: Dict[str, Any],
    *,
    include_scoped_hint: bool = True,
) -> List[str]:
    """Lines appended to ``replan_notes`` for the next ``generate_steps`` call."""
    notes: List[str] = []
    if gen_out is None:
        notes.append("preexec:generator_returned_none")
    elif isinstance(gen_out, dict):
        if gen_out.get("schema_gate_failed"):
            gd = str(gen_out.get("gate_detail") or "need_schema_refresh")[:900]
            notes.append(f"preexec_schema_gate:{gd}")
        if gen_out.get("generation_failed"):
            gd = str(gen_out.get("gate_detail") or "generation_failed")[:900]
            notes.append(f"preexec_generation_failed:{gd}")
        st = gen_out.get("steps")
        if isinstance(st, list) and not st and not gen_out.get("schema_gate_failed"):
            notes.append("preexec:empty_steps_array")
    if include_scoped_hint:
        hint = compact_scoped_schema_hint(context)
        if hint:
            notes.append("SCOPED_SCHEMA_FOR_REPAIR (authoritative slice):\n" + hint + "\n")
    return notes
