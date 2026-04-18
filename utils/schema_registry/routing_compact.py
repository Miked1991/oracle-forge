"""Compact schema summaries from `artifacts/schema_registry/*.json` for the routing LLM (Phase 3)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from agent.utils import canonical_db_name
from utils.schema_registry.builder import default_registry_path


def load_registry_json_optional(repo_root: Path, dataset_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not dataset_id or not str(dataset_id).strip():
        return None
    path = default_registry_path(str(dataset_id).strip(), repo_root)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def allowed_tables_by_database(registry: Dict[str, Any]) -> Dict[str, Set[str]]:
    """Map canonical DB name → set of table/collection names from registry engines."""
    out: Dict[str, Set[str]] = {}
    engines = registry.get("engines") or {}
    for eng_key, eng in engines.items():
        if not isinstance(eng, dict) or not eng.get("available"):
            continue
        db = canonical_db_name(str(eng_key))
        if not db:
            continue
        names: Set[str] = set()
        for t in eng.get("tables") or []:
            if isinstance(t, dict) and t.get("name"):
                names.add(str(t["name"]))
        for c in eng.get("collections") or []:
            if isinstance(c, dict) and c.get("name"):
                names.add(str(c["name"]))
        if names:
            out.setdefault(db, set()).update(names)
    return out


def compact_registry_routing_summary(
    registry: Dict[str, Any],
    available_databases: List[str],
    *,
    max_tables_per_engine: int = 40,
    max_line_chars: int = 220,
) -> str:
    """
    Short text block: one line per engine with table/collection names (registry is source of truth).
    """
    avail = {canonical_db_name(x) for x in available_databases if canonical_db_name(str(x))}
    allowed = allowed_tables_by_database(registry)
    lines: List[str] = []
    did = str(registry.get("dataset_id") or "").strip()
    if did:
        lines.append(f"dataset_id={did}")
    for db in sorted(avail):
        names = sorted(allowed.get(db, set()))
        if not names:
            continue
        chunk = names[:max_tables_per_engine]
        more = len(names) - len(chunk)
        suffix = f" (+{more} more)" if more > 0 else ""
        line = f"- {db}: {', '.join(chunk)}{suffix}"
        if len(line) > max_line_chars:
            line = line[: max_line_chars - 3] + "..."
        lines.append(line)
    return "\n".join(lines) if lines else ""


def filter_selected_tables_to_registry(
    selected_tables: Any,
    registry: Dict[str, Any],
    available_databases: List[str],
) -> Dict[str, List[str]]:
    """
    Keep only table/collection names that exist in the registry for each canonical DB.
    Unknown DB keys are dropped; unknown table names are dropped.
    """
    if not isinstance(selected_tables, dict):
        return {}
    allowed = allowed_tables_by_database(registry)
    avail = {canonical_db_name(x) for x in available_databases if canonical_db_name(str(x))}
    out: Dict[str, List[str]] = {}
    for raw_db, raw_names in selected_tables.items():
        db = canonical_db_name(str(raw_db))
        if db not in avail or db not in allowed:
            continue
        if not isinstance(raw_names, list):
            continue
        ok: List[str] = []
        seen: Set[str] = set()
        for item in raw_names:
            name = str(item).strip()
            if not name or name not in allowed[db]:
                continue
            if name not in seen:
                seen.add(name)
                ok.append(name)
        if ok:
            out[db] = ok
    return out
