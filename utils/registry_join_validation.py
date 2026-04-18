"""
Registry-backed join validation for SQL (Phase 6).

Uses ``verified_joins`` (dataset registry) plus per-table ``foreign_keys`` in ``schema_metadata``.
CTE aliases are excluded from physical table join edges.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set, Tuple

def _cte_aliases_from_sql(database: str, sql: str) -> Set[str]:
    """CTE names (lowercase) — copied from ``query_safety`` to avoid utils→agent imports."""
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return set()
    dialect = {"postgresql": "postgres", "duckdb": "duckdb", "sqlite": "sqlite"}.get(database, "postgres")
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return set()
    names: Set[str] = set()
    for with_expr in parsed.find_all(exp.With):
        for cte in with_expr.expressions or []:
            al = getattr(cte, "alias", None)
            if al is not None and str(al).strip():
                names.add(str(al).strip().lower())
    return names


def _env_join_validation_mode() -> str:
    return os.getenv("ORACLE_FORGE_REGISTRY_JOIN_VALIDATION", "auto").strip().lower()


def validation_registry_blob(schema_metadata: Dict[str, Any]) -> Dict[str, Any]:
    raw = schema_metadata.get("_validation_registry")
    return raw if isinstance(raw, dict) else {}


def edges_from_foreign_keys(database: str, schema_metadata: Dict[str, Any]) -> Set[frozenset]:
    """Undirected table pairs from FK metadata on ``schema_metadata[database].tables``."""
    meta = schema_metadata.get(database) or {}
    out: Set[frozenset] = set()
    for item in meta.get("tables") or []:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        child = str(item["name"]).strip().lower()
        for fk in item.get("foreign_keys") or []:
            if not isinstance(fk, dict):
                continue
            ref = fk.get("referenced_table") or fk.get("referencedTable")
            if isinstance(ref, str) and ref.strip():
                parent = ref.strip().lower()
                if parent != child:
                    out.add(frozenset({child, parent}))
    return out


def edges_from_verified_joins(schema_metadata: Dict[str, Any]) -> Set[frozenset]:
    vj = validation_registry_blob(schema_metadata).get("verified_joins")
    if not isinstance(vj, list):
        return set()
    out: Set[frozenset] = set()
    for item in vj:
        if not isinstance(item, dict):
            continue
        if item.get("left") is not None and item.get("right") is not None:
            a = str(item["left"]).strip().lower()
            b = str(item["right"]).strip().lower()
            if a and b:
                out.add(frozenset({a, b}))
            continue
        tabs = item.get("tables")
        if isinstance(tabs, list) and len(tabs) >= 2:
            a = str(tabs[0]).strip().lower()
            b = str(tabs[1]).strip().lower()
            if a and b:
                out.add(frozenset({a, b}))
    return out


def allowed_join_edges(database: str, schema_metadata: Dict[str, Any]) -> Set[frozenset]:
    """Union of FK-derived edges and registry ``verified_joins``."""
    fk = edges_from_foreign_keys(database, schema_metadata)
    vj = edges_from_verified_joins(schema_metadata)
    return fk | vj


def join_validation_should_apply(database: str, schema_metadata: Dict[str, Any]) -> bool:
    mode = _env_join_validation_mode()
    if mode in {"off", "false", "0", "no"}:
        return False
    allowed = allowed_join_edges(database, schema_metadata)
    return bool(allowed)


def _phys_table_name(node: Any, cte_names: Set[str]) -> Optional[str]:
    try:
        from sqlglot.expressions import Table
    except ImportError:
        return None
    if not isinstance(node, Table):
        return None
    name = str(node.name).strip().lower() if node.name else ""
    if not name or name in cte_names:
        return None
    return name


def _phys_from_from_root(from_root: Any, cte_names: Set[str]) -> Optional[str]:
    """Left side of first JOIN: usually ``From.this`` (Table or nested Join)."""
    try:
        from sqlglot.expressions import From, Join, Table
    except ImportError:
        return None
    if isinstance(from_root, From):
        return _phys_from_from_root(from_root.this, cte_names)
    if isinstance(from_root, Table):
        return _phys_table_name(from_root, cte_names)
    if isinstance(from_root, Join):
        # Nested FROM ( ... ) JOIN ...
        left = _phys_from_from_root(from_root.this, cte_names)
        if left:
            return left
        return _phys_table_name(from_root.expression, cte_names) if hasattr(from_root, "expression") else None
    return None


def extract_sql_join_edges(database: str, sql: str) -> Tuple[List[frozenset], str]:
    """
    Returns (undirected edges, status). status is ``ok`` or ``parse_failed``.
    Uses ``Select.args['joins']`` (sqlglot) so comma-FROM and explicit JOIN share one shape.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return [], "parse_failed"

    dialect = {"postgresql": "postgres", "duckdb": "duckdb", "sqlite": "sqlite"}.get(database, "postgres")
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return [], "parse_failed"

    cte_names = _cte_aliases_from_sql(database, sql)
    selects: List[exp.Select] = []
    if isinstance(parsed, exp.Select):
        selects.append(parsed)
    for u in parsed.find_all(exp.Union):
        if isinstance(u.this, exp.Select):
            selects.append(u.this)
        if isinstance(u.expression, exp.Select):
            selects.append(u.expression)

    edges: List[frozenset] = []
    for sel in selects:
        fr = sel.args.get("from")
        if not fr:
            continue
        joins = sel.args.get("joins") or []
        if not joins:
            continue
        left_base = _phys_from_from_root(fr, cte_names)
        prev_right: Optional[str] = None
        for join in joins:
            if not isinstance(join, exp.Join):
                continue
            right = _phys_table_name(join.this, cte_names)
            cur_left = prev_right if prev_right is not None else left_base
            if cur_left and right:
                edges.append(frozenset({cur_left, right}))
            if right:
                prev_right = right

    return edges, "ok"


def validate_sql_join_registry(
    database: str,
    sql: str,
    schema_metadata: Dict[str, Any],
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    When registry/FK metadata defines at least one allowed edge, reject JOINs whose
    table pairs are not in that set (undirected).
    """
    detail: Dict[str, Any] = {"join_validation": "skipped", "reason": ""}
    if not join_validation_should_apply(database, schema_metadata):
        detail["reason"] = "no_allowed_edges_or_mode_off"
        return True, "ok", detail

    allowed = allowed_join_edges(database, schema_metadata)
    edges, status = extract_sql_join_edges(database, sql)
    detail["join_validation"] = "checked"
    detail["parse_status"] = status
    detail["allowed_edge_count"] = len(allowed)
    detail["join_edges_found"] = [sorted(list(e)) for e in edges]

    if status != "ok":
        detail["reason"] = "join_parse_failed_non_strict"
        return True, "ok", detail

    if not edges:
        detail["reason"] = "no_join_edges_in_query"
        return True, "ok", detail

    bad: List[List[str]] = []
    for e in edges:
        if e not in allowed:
            bad.append(sorted(list(e)))

    if bad:
        sample = sorted([sorted(list(x)) for x in list(allowed)[:12]])
        detail["reason"] = "disallowed_join"
        detail["rejected_pairs"] = bad
        return (
            False,
            f"disallowed_join: pairs {bad} not in registry/FK allowlist (sample allowed: {sample})",
            detail,
        )

    detail["reason"] = "all_joins_allowed"
    return True, "ok", detail
