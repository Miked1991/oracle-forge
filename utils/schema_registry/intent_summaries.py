"""Heuristic intent_summary text for registry tables/collections (no LLM; schema + row counts)."""

from __future__ import annotations

from typing import Any, Dict, List


def enrich_registry_intent_summaries(registry: Dict[str, Any]) -> None:
    """
    Populate ``intent_summary`` and clear ``intent_summary_pending`` for every table and
    collection. Adds ``dataset_intent_summary`` at the registry root from cross-engine names.
    Mutates ``registry`` in place.
    """
    dataset_id = str(registry.get("dataset_id") or "").strip() or "unknown"
    engines = registry.get("engines") or {}
    per_engine_one_liners: List[str] = []

    for eng_name, eng in engines.items():
        if not isinstance(eng, dict) or not eng.get("available"):
            continue
        bits: List[str] = []
        for tbl in eng.get("tables") or []:
            if isinstance(tbl, dict):
                _fill_table_or_collection_intent(tbl, str(eng_name), "table", dataset_id)
                bits.append(tbl.get("name", ""))
        for coll in eng.get("collections") or []:
            if isinstance(coll, dict):
                _fill_table_or_collection_intent(coll, str(eng_name), "collection", dataset_id)
                bits.append(coll.get("name", ""))
        if bits:
            per_engine_one_liners.append(f"{eng_name}: {', '.join(sorted(set(b for b in bits if b)))}")

    registry["dataset_intent_summary"] = _dataset_level_blurb(dataset_id, per_engine_one_liners, engines)


def _col_names(columns: Any, limit: int = 14) -> List[str]:
    out: List[str] = []
    if not isinstance(columns, list):
        return out
    for c in columns:
        if isinstance(c, dict) and c.get("name"):
            out.append(str(c["name"]))
        if len(out) >= limit:
            break
    return out


def _fill_table_or_collection_intent(
    obj: Dict[str, Any],
    engine: str,
    kind: str,
    dataset_id: str,
) -> None:
    name = str(obj.get("name") or "unknown")
    cols = obj.get("columns") or []
    colnames = _col_names(cols, 16)
    n = obj.get("row_count_estimate")
    pk = obj.get("primary_key") or []
    fks = obj.get("foreign_keys") or []
    extra_cols = max(0, len(cols) - len(colnames)) if isinstance(cols, list) else 0

    row_part = f"approximately {int(n):,} rows" if isinstance(n, (int, float)) and n >= 0 else "row count unknown"
    pk_part = f"primary key on ({', '.join(pk)})" if pk else "no primary key in metadata"
    fk_part = ""
    if isinstance(fks, list) and fks:
        fk_part = f" {len(fks)} foreign-key hint(s) in metadata."

    col_part = ", ".join(colnames)
    if extra_cols:
        col_part += f", +{extra_cols} more column(s)"

    role = (
        "Document collection for nested or text-heavy fields; filter and aggregate in Mongo pipelines."
        if kind == "collection"
        else "Relational table for SQL joins, filters, and aggregates."
    )

    obj["intent_summary"] = (
        f"Dataset `{dataset_id}` — [{engine}] {kind} `{name}`: {row_part}. "
        f"{pk_part}.{fk_part} "
        f"Key fields: {col_part}. {role}"
    )[:2000]

    obj["intent_summary_pending"] = False


def _dataset_level_blurb(dataset_id: str, per_engine: List[str], engines: Dict[str, Any]) -> str:
    lines = [
        f"Registry for benchmark dataset `{dataset_id}`: combined schema from all reachable engines.",
        "Engine overview: " + (" | ".join(per_engine) if per_engine else "no objects introspected."),
    ]
    mongo = engines.get("mongodb") if isinstance(engines.get("mongodb"), dict) else {}
    if isinstance(mongo, dict) and mongo.get("available") and not (mongo.get("collections") or []):
        lines.append(
            "MongoDB connected but this logical database has no collections yet (empty or not seeded); "
            "other databases on the same Mongo host may still hold data under different database names."
        )
    return " ".join(lines)[:4000]
