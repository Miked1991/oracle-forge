"""
Validate LLM- or heuristic-generated SQL and MongoDB pipelines before execution.

Blocks obvious DDL/DML/injection patterns and optionally enforces table/collection allowlists
from runtime schema_metadata (see kb/architecture/tool_scoping_philosophy.md).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_MAX_SQL_LEN = int(os.getenv("ORACLE_FORGE_MAX_SQL_CHARS", "12000"))
_MAX_MONGO_STAGES = int(os.getenv("ORACLE_FORGE_MAX_MONGO_STAGES", "32"))


def _strict_allowlist() -> bool:
    return os.getenv("ORACLE_FORGE_SQL_STRICT_ALLOWLIST", "true").lower() in {"1", "true", "yes", "on"}


_SQL_FORBIDDEN = re.compile(
    r"\b("
    r"drop|delete|insert|update|alter|create|truncate|grant|revoke|merge|replace|"
    r"attach|detach|pragma|copy|call|execute\s+immediate|into\s+outfile|load\s+data"
    r")\b",
    re.IGNORECASE | re.DOTALL,
)


def _allowed_tables_for_db(database: str, schema_metadata: Dict[str, Any]) -> Set[str]:
    meta = schema_metadata.get(database) or {}
    raw = meta.get("tables") or []
    names: Set[str] = set()
    for item in raw:
        if isinstance(item, dict) and item.get("name"):
            names.add(str(item["name"]).strip().lower())
        elif isinstance(item, str):
            names.add(item.strip().lower())
    return names


def _allowed_collections_for_db(database: str, schema_metadata: Dict[str, Any]) -> Set[str]:
    meta = schema_metadata.get(database) or {}
    raw = meta.get("collections") or []
    names: Set[str] = set()
    for item in raw:
        if isinstance(item, dict) and item.get("name"):
            names.add(str(item["name"]).strip().lower())
        elif isinstance(item, str):
            names.add(item.strip().lower())
    return names


def _extract_sql_tables_regex(sql: str) -> Set[str]:
    """Best-effort table names from FROM / JOIN (regex). May treat CTE names as tables."""
    found: Set[str] = set()
    for m in re.finditer(r'(?is)\b(?:from|join)\s+["`]?(?:public\.)?(\w+)["`]?', sql):
        found.add(m.group(1).lower())
    return found


def _cte_aliases_from_sql(database: str, sql: str) -> Set[str]:
    """All CTE names (lowercase) for subtracting from physical table checks."""
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


def _physical_sql_table_names(database: str, sql: str) -> Set[str]:
    """
    Lowercase physical base table names referenced in SQL, excluding CTE aliases.
    Falls back to regex extraction when sqlglot is missing or parsing fails.
    """
    dialect = {"postgresql": "postgres", "duckdb": "duckdb", "sqlite": "sqlite"}.get(database, "postgres")
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return _extract_sql_tables_regex(sql)
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return _extract_sql_tables_regex(sql)
    cte_names: Set[str] = set()
    for with_expr in parsed.find_all(exp.With):
        for cte in with_expr.expressions or []:
            al = getattr(cte, "alias", None)
            if al is not None and str(al).strip():
                cte_names.add(str(al).strip().lower())
    physical: Set[str] = set()
    for tbl in parsed.find_all(exp.Table):
        name = str(tbl.name).strip().lower() if tbl.name else ""
        if name and name not in cte_names:
            physical.add(name)
    return physical


def _strict_sql_columns() -> bool:
    return os.getenv("ORACLE_FORGE_STRICT_SQL_COLUMNS", "true").lower() in {"1", "true", "yes", "on"}


def _table_to_column_types(database: str, schema_metadata: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Lowercase table -> lowercase column -> lowercased type string from schema."""
    out: Dict[str, Dict[str, str]] = {}
    meta = schema_metadata.get(database) or {}
    for item in meta.get("tables") or []:
        if not isinstance(item, dict):
            continue
        t = str(item.get("name", "")).strip().lower()
        if not t:
            continue
        fields = item.get("fields") or {}
        if not isinstance(fields, dict):
            continue
        out[t] = {str(k).lower(): str(v).lower() for k, v in fields.items()}
    return out


def _is_textish_sql_type(type_str: str) -> bool:
    t = (type_str or "").lower()
    if not t:
        return False
    if "timestamp" in t or "time with" in t or t == "date":
        return False
    return any(x in t for x in ("text", "char", "varchar", "string"))


def _validate_text_column_vs_date_literal(
    database: str,
    sql: str,
    schema_metadata: Dict[str, Any],
) -> Tuple[bool, str]:
    """
    PostgreSQL: comparing TEXT-stored date columns to DATE literals fails at runtime (42883).
    Reject at validation when schema lists the column as text-like.
    """
    if database != "postgresql":
        return True, "ok"
    if os.getenv("ORACLE_FORGE_SQL_CHECK_TEXT_DATE_COMPARE", "true").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return True, "ok"
    col_types = _table_to_column_types(database, schema_metadata)
    if not any(col_types.values()):
        return True, "ok"
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return True, "ok"
    try:
        expr = sqlglot.parse_one(sql, read="postgres")
    except Exception:
        return True, "ok"

    alias_to_physical: Dict[str, str] = {}
    for tbl in expr.find_all(exp.Table):
        phys = str(tbl.name).lower()
        al = str(tbl.alias).lower() if tbl.alias else phys
        alias_to_physical[al] = phys
        alias_to_physical[phys] = phys

    def _other_side_date_like(node: exp.Expression) -> bool:
        if isinstance(node, exp.Cast):
            dest = str(node.to) if node.to else ""
            return "date" in dest.lower() or "timestamp" in dest.lower()
        if isinstance(node, exp.Date):
            return True
        return False

    for cls in (exp.GT, exp.GTE, exp.LT, exp.LTE, exp.EQ):
        for node in expr.find_all(cls):
            left, right = node.left, node.right
            for col_side, date_side in ((left, right), (right, left)):
                if not isinstance(col_side, exp.Column):
                    continue
                if not _other_side_date_like(date_side):
                    continue
                tref = col_side.table
                if tref is None or not str(tref).strip():
                    continue
                phys = alias_to_physical.get(str(tref).lower())
                cname = str(col_side.name).lower()
                if not phys or not cname:
                    continue
                tmap = col_types.get(phys)
                if not tmap:
                    continue
                typ = tmap.get(cname, "")
                if typ and _is_textish_sql_type(typ):
                    return (
                        False,
                        f"text_date_compare: column `{phys}.{cname}` is typed `{typ}` in schema — "
                        f"compare using `(alias).{cname}::date` or `CAST((alias).{cname} AS DATE)` before DATE literals",
                    )
    return True, "ok"


def _table_to_columns(database: str, schema_metadata: Dict[str, Any]) -> Dict[str, Set[str]]:
    """Lowercase table name -> set of lowercase column names."""
    out: Dict[str, Set[str]] = {}
    meta = schema_metadata.get(database) or {}
    for item in meta.get("tables") or []:
        if not isinstance(item, dict):
            continue
        t = str(item.get("name", "")).strip().lower()
        if not t:
            continue
        fields = item.get("fields") or {}
        if isinstance(fields, dict) and fields:
            out[t] = {str(k).lower() for k in fields.keys()}
    return out


def _select_projection_output_names(sel: Any) -> Set[str]:
    """Output column names from a SELECT (best-effort; supports CTE validation)."""
    try:
        from sqlglot import exp
    except ImportError:
        return set()
    if not isinstance(sel, exp.Select):
        return set()
    names: Set[str] = set()
    for e in sel.expressions:
        if isinstance(e, exp.Alias):
            names.add(str(e.alias).lower())
        elif isinstance(e, exp.Column):
            names.add(str(e.name).lower())
    return names


def _cte_output_column_sets(expr: Any) -> Dict[str, Set[str]]:
    """CTE alias -> projected column names (lowercase)."""
    try:
        from sqlglot import exp
    except ImportError:
        return {}
    out: Dict[str, Set[str]] = {}
    for with_expr in expr.find_all(exp.With):
        for cte in with_expr.expressions or []:
            al = getattr(cte, "alias", None)
            if al is None or not str(al).strip():
                continue
            key = str(al).strip().lower()
            inner = cte.this
            if isinstance(inner, exp.Select):
                out[key] = _select_projection_output_names(inner)
            elif isinstance(inner, exp.Subquery) and isinstance(inner.this, exp.Select):
                out[key] = _select_projection_output_names(inner.this)
    return out


def _validate_sql_columns(
    database: str,
    sql: str,
    schema_metadata: Dict[str, Any],
) -> Tuple[bool, str]:
    if not _strict_sql_columns():
        return True, "ok"
    table_cols = _table_to_columns(database, schema_metadata)
    if not any(table_cols.values()):
        return True, "ok"
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return True, "ok"
    dialect = {"postgresql": "postgres", "duckdb": "duckdb", "sqlite": "sqlite"}.get(database, "postgres")
    try:
        expr = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return True, "ok"
    cte_names_lower = {str(getattr(cte, "alias", "") or "").strip().lower() for with_expr in expr.find_all(exp.With) for cte in (with_expr.expressions or []) if str(getattr(cte, "alias", "") or "").strip()}
    alias_to_physical: Dict[str, str] = {}
    for tbl in expr.find_all(exp.Table):
        phys = str(tbl.name).lower()
        al = str(tbl.alias).lower() if tbl.alias else phys
        alias_to_physical[al] = phys
        alias_to_physical[phys] = phys
        # FROM clause may reference a CTE name as if it were a table
        if phys in cte_names_lower:
            alias_to_physical[phys] = phys
    cte_outputs = _cte_output_column_sets(expr)
    table_cols_ext: Dict[str, Set[str]] = dict(table_cols)
    for cte_name, proj_cols in cte_outputs.items():
        table_cols_ext[cte_name] = proj_cols
    tables_in_query = set(alias_to_physical.values())
    union_cols: Set[str] = set()
    for t in tables_in_query:
        union_cols |= table_cols_ext.get(t, set())
    for col in expr.find_all(exp.Column):
        if isinstance(col.this, exp.Star):
            continue
        cname = str(col.name).lower()
        tref = col.table
        if tref is not None and str(tref).strip():
            tkey = str(tref).lower()
            phys = alias_to_physical.get(tkey, tkey)
            allowed = table_cols_ext.get(phys)
            if not allowed:
                return False, f"unknown_columns:no_schema_for_table:{phys}"
            if cname not in allowed:
                return False, f"unknown_columns:{phys}.{cname} (allowed sample: {sorted(allowed)[:20]})"
        else:
            if cname not in union_cols:
                matches = [t for t in tables_in_query if cname in table_cols_ext.get(t, set())]
                if len(matches) != 1:
                    return False, f"unknown_columns:ambiguous_or_missing:{cname}"
    return True, "ok"


def _mongo_lookup_collections(pipeline: Any) -> List[str]:
    """Collection names referenced by ``$lookup.from`` (lowercase)."""
    out: List[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            lk = node.get("$lookup")
            if isinstance(lk, dict):
                frm = lk.get("from")
                if isinstance(frm, str) and frm.strip():
                    out.append(frm.strip().lower())
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for x in node:
                walk(x)

    walk(pipeline)
    return out


def validate_sql(
    database: str,
    sql: str,
    schema_metadata: Dict[str, Any],
) -> Tuple[bool, str]:
    if not isinstance(sql, str) or not sql.strip():
        return False, "empty_sql"
    s = sql.strip()
    if len(s) > _MAX_SQL_LEN:
        return False, "sql_too_long"
    if ";" in s.rstrip(";"):
        return False, "multiple_statements_not_allowed"
    if _SQL_FORBIDDEN.search(s):
        return False, "forbidden_sql_keyword"
    allowed = _allowed_tables_for_db(database, schema_metadata)
    if _strict_allowlist() and allowed:
        tables = _physical_sql_table_names(database, s)
        cte_aliases = _cte_aliases_from_sql(database, s)
        if tables:
            unknown = tables - allowed - cte_aliases
            if unknown:
                return False, f"unknown_tables:{sorted(unknown)}"
    ok_cols, msg_cols = _validate_sql_columns(database, s, schema_metadata)
    if not ok_cols:
        return False, msg_cols
    ok_td, msg_td = _validate_text_column_vs_date_literal(database, s, schema_metadata)
    if not ok_td:
        return False, msg_td
    from utils.registry_join_validation import validate_sql_join_registry

    ok_j, msg_j, _detail = validate_sql_join_registry(database, s, schema_metadata)
    if not ok_j:
        return False, msg_j
    return True, "ok"


def validate_mongo_pipeline(
    database: str,
    collection: str,
    pipeline: Any,
    schema_metadata: Dict[str, Any],
) -> Tuple[bool, str]:
    if not isinstance(collection, str) or not collection.strip():
        return False, "empty_collection"
    if not isinstance(pipeline, list):
        return False, "pipeline_must_be_list"
    if len(pipeline) > _MAX_MONGO_STAGES:
        return False, "too_many_pipeline_stages"
    allowed = _allowed_collections_for_db(database, schema_metadata)
    if _strict_allowlist() and allowed and collection.strip().lower() not in allowed:
        return False, f"unknown_collection:{collection}"
    if _strict_allowlist() and allowed:
        for c in _mongo_lookup_collections(pipeline):
            if c not in allowed:
                return False, f"unknown_lookup_collection:{c}"
    raw = json.dumps(pipeline)
    if re.search(r"\$where\b", raw) and '"$where"' in raw:
        return False, "where_clause_not_allowed"
    return True, "ok"


def llm_raw_step_to_validator_step(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map LLM JSON step (flat ``sql`` / ``collection`` keys) to validate_step_payload shape."""
    from agent.utils import canonical_db_name

    if not isinstance(raw, dict):
        return None
    db = canonical_db_name(str(raw.get("database", "")))
    if not db:
        return None
    d = (raw.get("dialect") or "").strip().lower()
    if db == "mongodb" or d == "mongodb_aggregation":
        return {
            "database": db,
            "dialect": "mongodb_aggregation",
            "query_payload": {
                "collection": str(raw.get("collection") or ""),
                "pipeline": raw.get("pipeline"),
            },
        }
    sql = raw.get("sql")
    if not isinstance(sql, str):
        sql = ""
    return {
        "database": db,
        "dialect": "sql",
        "query_payload": {"sql": sql},
    }


def validate_llm_generated_steps(
    raw_steps: List[Any],
    schema_metadata: Dict[str, Any],
    *,
    validation_log_repo_root: Optional[Path] = None,
    validation_log_question: Optional[str] = None,
    validation_log_dataset_id: Optional[str] = None,
) -> Tuple[bool, List[str]]:
    """
    Validate LLM ``steps`` list (before mapping to PlanStep). Returns (all_ok, error_messages).

    When ``validation_log_repo_root`` is set, appends one JSON line to
    ``logs/pre_execution_validator.jsonl`` (disable with ``ORACLE_FORGE_PREEXEC_VALIDATION_LOG=false``).
    """
    errors: List[str] = []
    if not isinstance(raw_steps, list):
        errs = ["steps_not_a_list"]
        if validation_log_repo_root is not None:
            from utils.pre_execution_validation_log import append_pre_execution_validation_log

            append_pre_execution_validation_log(
                validation_log_repo_root,
                {
                    "question": (validation_log_question or "")[:4000],
                    "dataset_id": validation_log_dataset_id,
                    "all_ok": False,
                    "errors": errs,
                    "step_count": 0,
                },
            )
        return False, errs
    for i, raw in enumerate(raw_steps):
        step = llm_raw_step_to_validator_step(raw if isinstance(raw, dict) else {})
        if step is None:
            errors.append(f"step {i + 1}: invalid_or_missing_database")
            continue
        ok, msg = validate_step_payload(step, schema_metadata)
        if not ok:
            errors.append(f"step {i + 1} ({step.get('database')}): {msg}")
    all_ok = len(errors) == 0
    if validation_log_repo_root is not None:
        from utils.pre_execution_validation_log import append_pre_execution_validation_log

        append_pre_execution_validation_log(
            validation_log_repo_root,
            {
                "question": (validation_log_question or "")[:4000],
                "dataset_id": validation_log_dataset_id,
                "all_ok": all_ok,
                "errors": errors,
                "step_count": len(raw_steps),
            },
        )
    return (all_ok, errors)


def validate_step_payload(
    step: Dict[str, Any],
    schema_metadata: Dict[str, Any],
) -> Tuple[bool, str]:
    db = (step.get("database") or "").strip().lower()
    dialect = (step.get("dialect") or "").strip().lower()
    payload = step.get("query_payload") or {}
    if dialect == "mongodb_aggregation":
        col = str(payload.get("collection") or "")
        pipe = payload.get("pipeline")
        return validate_mongo_pipeline(db, col, pipe, schema_metadata)
    sql = payload.get("sql")
    if isinstance(sql, str):
        return validate_sql(db, sql, schema_metadata)
    return False, "missing_sql_or_pipeline"
