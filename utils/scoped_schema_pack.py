"""Phase 4: table-scoped schema packages with registry intent summaries for query generation."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from agent.utils import canonical_db_name


def _meta_tables_named(meta_db: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for item in meta_db.get("tables") or []:
        if isinstance(item, dict) and item.get("name"):
            out.append(str(item["name"]))
    return out


def _meta_collections_named(meta_db: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for item in meta_db.get("collections") or []:
        if isinstance(item, dict) and item.get("name"):
            out.append(str(item["name"]))
    return out


def _find_meta_table(meta_db: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    for item in meta_db.get("tables") or []:
        if isinstance(item, dict) and str(item.get("name", "")) == name:
            return item
    return None


def _find_meta_collection(meta_db: Dict[str, Any], name: str) -> Optional[Dict[str, Any]]:
    for item in meta_db.get("collections") or []:
        if isinstance(item, dict) and str(item.get("name", "")) == name:
            return item
    return None


def _find_registry_object(registry: Dict[str, Any], db: str, name: str) -> Optional[Dict[str, Any]]:
    eng = (registry.get("engines") or {}).get(db)
    if not isinstance(eng, dict) or not eng.get("available"):
        return None
    for t in eng.get("tables") or []:
        if isinstance(t, dict) and str(t.get("name", "")) == name:
            return t
    for c in eng.get("collections") or []:
        if isinstance(c, dict) and str(c.get("name", "")) == name:
            return c
    return None


def _field_keys_from_meta(item: Optional[Dict[str, Any]], max_fields: int = 120) -> List[str]:
    if not isinstance(item, dict):
        return []
    fields = item.get("fields")
    if isinstance(fields, dict):
        return list(fields.keys())[:max_fields]
    return []


def _field_names_from_registry_row(row: Dict[str, Any], max_fields: int = 120) -> List[str]:
    cols = row.get("columns") or []
    if not isinstance(cols, list):
        return []
    out: List[str] = []
    for c in cols:
        if isinstance(c, dict) and c.get("name"):
            out.append(str(c["name"]))
        if len(out) >= max_fields:
            break
    return out


def _table_bundle_entry(
    db: str,
    name: str,
    registry: Optional[Dict[str, Any]],
    meta_db: Dict[str, Any],
) -> Dict[str, Any]:
    meta_row = _find_meta_table(meta_db, name)
    reg_row = _find_registry_object(registry, db, name) if registry else None
    fields = _field_names_from_registry_row(reg_row) if reg_row else _field_keys_from_meta(meta_row)
    entry: Dict[str, Any] = {"name": name, "fields": fields}
    if reg_row:
        if reg_row.get("intent_summary"):
            entry["intent_summary"] = str(reg_row["intent_summary"])[:4000]
        if reg_row.get("row_count_estimate") is not None:
            entry["row_count_estimate"] = reg_row["row_count_estimate"]
        pk = reg_row.get("primary_key")
        if isinstance(pk, list) and pk:
            entry["primary_key"] = pk
        fks = reg_row.get("foreign_keys")
        if isinstance(fks, list) and fks:
            entry["foreign_keys"] = fks[:32]
        entry["source"] = "registry"
    else:
        entry["source"] = "metadata"
    return entry


def _collection_bundle_entry(
    db: str,
    name: str,
    registry: Optional[Dict[str, Any]],
    meta_db: Dict[str, Any],
) -> Dict[str, Any]:
    meta_row = _find_meta_collection(meta_db, name)
    reg_row = _find_registry_object(registry, db, name) if registry else None
    fields = _field_names_from_registry_row(reg_row) if reg_row else _field_keys_from_meta(meta_row)
    entry: Dict[str, Any] = {"name": name, "fields": fields}
    if reg_row:
        if reg_row.get("intent_summary"):
            entry["intent_summary"] = str(reg_row["intent_summary"])[:4000]
        if reg_row.get("row_count_estimate") is not None:
            entry["row_count_estimate"] = reg_row["row_count_estimate"]
        entry["source"] = "registry"
    else:
        entry["source"] = "metadata"
    return entry


def schema_metadata_stub_from_registry(registry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a ``schema_metadata``-shaped dict from the canonical schema registry so offline tools
    (e.g. ``routing_probe``) can run the same scoped-pack pipeline without a live MCP introspection.
    """
    out: Dict[str, Any] = {}
    for eng_key, eng in (registry.get("engines") or {}).items():
        if not isinstance(eng, dict) or not eng.get("available"):
            continue
        db = canonical_db_name(str(eng_key))
        if not db:
            continue
        tables: List[Dict[str, Any]] = []
        for t in eng.get("tables") or []:
            if not isinstance(t, dict) or not t.get("name"):
                continue
            fields: Dict[str, str] = {}
            for col in t.get("columns") or []:
                if isinstance(col, dict) and col.get("name"):
                    fields[str(col["name"])] = str(col.get("data_type") or "")
            row: Dict[str, Any] = {"name": str(t["name"]), "fields": fields}
            fks = t.get("foreign_keys")
            if isinstance(fks, list) and fks:
                row["foreign_keys"] = fks[:32]
            tables.append(row)
        collections: List[Dict[str, Any]] = []
        for c in eng.get("collections") or []:
            if not isinstance(c, dict) or not c.get("name"):
                continue
            fields: Dict[str, str] = {}
            for col in c.get("fields") or c.get("columns") or []:
                if isinstance(col, dict) and col.get("name"):
                    fields[str(col["name"])] = str(col.get("data_type") or "")
            collections.append({"name": str(c["name"]), "fields": fields})
        out[db] = {"tables": tables, "collections": collections}
    out["_validation_registry"] = {
        "verified_joins": list(registry.get("verified_joins") or []),
        "dataset_id": registry.get("dataset_id"),
    }
    return out


def should_apply_table_scope(selected_tables: Any, selected_databases: List[str]) -> bool:
    """True when routing supplied at least one non-empty table list for a selected engine."""
    if not isinstance(selected_tables, dict) or not selected_databases:
        return False
    sel = {canonical_db_name(d) for d in selected_databases if canonical_db_name(str(d))}
    for db in sel:
        names = selected_tables.get(db)
        if names is None:
            continue
        if isinstance(names, list) and len([x for x in names if str(x).strip()]) > 0:
            return True
    return False


def names_to_include_for_engine(
    db: str,
    selected_tables: Dict[str, List[str]],
    meta_db: Dict[str, Any],
    *,
    scope_active: bool,
) -> tuple[Set[str], Set[str]]:
    """
    Returns (table_names, collection_names) to include for this engine.
    When scope_active and this DB has a non-empty selected list, use intersection with metadata.
    When scope_active and this DB has empty/missing list, include all metadata names (other DBs may be narrowed).
    When not scope_active, include all.
    """
    all_tables = set(_meta_tables_named(meta_db))
    all_colls = set(_meta_collections_named(meta_db))
    if not scope_active:
        return all_tables, all_colls
    raw = selected_tables.get(db)
    if raw is None or not isinstance(raw, list) or not [x for x in raw if str(x).strip()]:
        return all_tables, all_colls
    want = {str(x).strip() for x in raw if str(x).strip()}
    t_inter = want & all_tables
    c_inter = want & all_colls
    if want and not t_inter and not c_inter and (all_tables or all_colls):
        return all_tables, all_colls
    return t_inter, c_inter


def build_scoped_schema_bundle(
    schema_metadata: Dict[str, Any],
    registry: Optional[Dict[str, Any]],
    selected_databases: List[str],
    selected_tables: Dict[str, List[str]],
    dataset_id: Optional[str],
    playbook: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a schema bundle containing only selected tables per engine, with intent summaries from the
    registry when available. Engines in ``selected_databases`` are included; within each engine,
    table/collection lists follow Phase 4 scope rules (see ``names_to_include_for_engine``).
    """
    scope_active = should_apply_table_scope(selected_tables, selected_databases)
    bundle: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "scoped": bool(scope_active),
        "scoped_tables": {k: list(v) for k, v in (selected_tables or {}).items() if isinstance(v, list)},
        "engines": {},
    }
    for raw in selected_databases:
        db = canonical_db_name(str(raw))
        if not db:
            continue
        meta_db = schema_metadata.get(db) or {}
        if not isinstance(meta_db, dict):
            meta_db = {}
        tnames, cnames = names_to_include_for_engine(
            db, selected_tables or {}, meta_db, scope_active=scope_active
        )
        tables_out: List[Dict[str, Any]] = []
        for t in sorted(tnames):
            tables_out.append(_table_bundle_entry(db, t, registry, meta_db))
        colls_out: List[Dict[str, Any]] = []
        for c in sorted(cnames):
            colls_out.append(_collection_bundle_entry(db, c, registry, meta_db))
        bundle["engines"][db] = {"tables": tables_out, "collections": colls_out}
    if playbook:
        eng = playbook.get("engines") or {}
        roles: Dict[str, str] = {}
        for name, block in eng.items():
            if isinstance(block, dict) and (block.get("role") or "").strip():
                roles[str(name)] = str(block.get("role", "")).strip()[:1200]
        bundle["benchmark_playbook"] = {
            "summary": str(playbook.get("summary", ""))[:8000],
            "engine_roles": roles,
            "suggest_engines_order": list(playbook.get("suggest_engines_order") or [])[:20],
        }
    return bundle


def scoped_schema_bundle_json(bundle: Dict[str, Any], max_chars: int = 14000) -> str:
    text = json.dumps(bundle, ensure_ascii=False)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def rebuild_with_scoped_pack(
    context: Dict[str, Any],
    available_databases: List[str],
    dataset_id: Optional[str],
    *,
    repo_root: Path,
) -> None:
    """
    Set ``schema_bundle`` / ``schema_bundle_json`` on context: use table-scoped + registry intents
    when routing provided ``selected_tables``; otherwise fall back to engine-only bundle.
    Updates ``runtime/schema_metadata.json`` in context_layers when present.
    """
    import os

    from utils.schema_bundle import build_schema_bundle, schema_bundle_json
    from utils.schema_registry.builder import default_registry_path
    from utils.schema_registry.routing_compact import load_registry_json_optional
    from utils.scoped_schema_log import log_scoped_schema_event

    t0 = time.perf_counter()
    sm = context.get("schema_metadata") or {}
    pb = context.get("dataset_playbook")
    playbook_arg = pb if isinstance(pb, dict) and pb else None
    lg = context.get("llm_guidance") if isinstance(context.get("llm_guidance"), dict) else {}
    sel_dbs = lg.get("selected_databases") if isinstance(lg.get("selected_databases"), list) else []
    if not sel_dbs:
        sel_dbs = list(available_databases)
    sel_dbs = [canonical_db_name(x) for x in sel_dbs if canonical_db_name(str(x))]
    seen: List[str] = []
    for d in sel_dbs:
        if d not in seen:
            seen.append(d)
    sel_dbs = seen

    st = lg.get("selected_tables")
    selected_tables = st if isinstance(st, dict) else {}

    registry = load_registry_json_optional(repo_root, str(dataset_id).strip() if dataset_id else None)

    use_scoped = should_apply_table_scope(selected_tables, sel_dbs) and registry is not None
    if use_scoped:
        bundle = build_scoped_schema_bundle(
            sm,
            registry,
            sel_dbs,
            selected_tables,
            dataset_id,
            playbook=playbook_arg,
        )
        context["schema_bundle_mode"] = "scoped_tables"
    elif should_apply_table_scope(selected_tables, sel_dbs) and registry is None:
        bundle = build_scoped_schema_bundle(
            sm,
            None,
            sel_dbs,
            selected_tables,
            dataset_id,
            playbook=playbook_arg,
        )
        context["schema_bundle_mode"] = "scoped_tables_metadata_only"
    else:
        bundle = build_schema_bundle(sm, sel_dbs, dataset_id, playbook=playbook_arg)
        context["schema_bundle_mode"] = "engines_only"

    context["schema_bundle"] = bundle
    max_chars = int(os.getenv("ORACLE_FORGE_QUERY_GEN_MAX_SCHEMA_CHARS", "14000"))
    if bundle.get("scoped"):
        context["schema_bundle_json"] = scoped_schema_bundle_json(bundle, max_chars=max_chars)
    else:
        context["schema_bundle_json"] = schema_bundle_json(bundle, max_chars=max_chars)

    layer = context.get("context_layers")
    if isinstance(layer, dict):
        sm_layer = layer.get("schema_metadata")
        if isinstance(sm_layer, dict) and "runtime/schema_metadata.json" in sm_layer:
            sm_layer["runtime/schema_metadata.json"] = json.dumps(sm, ensure_ascii=False)

    duration_ms = int((time.perf_counter() - t0) * 1000)
    refs: List[str] = []
    if dataset_id and str(dataset_id).strip():
        rp = default_registry_path(str(dataset_id).strip(), repo_root)
        if rp.is_file():
            try:
                refs.append(str(rp.relative_to(repo_root)))
            except ValueError:
                refs.append(str(rp))
    log_scoped_schema_event(
        {
            "phase": "scoped_schema",
            "dataset_id": str(dataset_id).strip() if dataset_id else None,
            "schema_bundle_mode": context.get("schema_bundle_mode"),
            "selected_databases": sel_dbs,
            "selected_tables": selected_tables,
            "bundle_json_chars": len(context.get("schema_bundle_json") or ""),
            "status": "ok",
            "duration_ms": duration_ms,
            "input_artifact_refs": refs,
        },
        repo_root=repo_root,
    )
