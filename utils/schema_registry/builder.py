"""Build canonical `artifacts/schema_registry/<dataset>.json` from live DBs + dataset config."""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.dataset_profiles import DatasetProfile, discover_dab_connection_paths, load_dataset_profile

from utils.schema_registry.env import (
    load_registry_environment,
    mongodb_database_name,
    resolved_mongodb_uri,
    resolved_postgres_dsn,
)
from utils.schema_registry.introspect import (
    introspect_duckdb,
    introspect_mongodb,
    introspect_postgresql_sync,
    introspect_sqlite,
    run_postgres,
)
from utils.schema_registry.intent_summaries import enrich_registry_intent_summaries
from utils.schema_registry.schema_log import log_schema_registry_event

SCHEMA_REGISTRY_VERSION = "1.0"


def default_registry_path(dataset_id: str, repo_root: Path) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in dataset_id.strip())
    return repo_root / "artifacts" / "schema_registry" / f"{safe}.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redact_dsn(dsn: str) -> str:
    if "@" not in dsn:
        return "postgresql://…"
    try:
        head, tail = dsn.split("@", 1)
        user = head.split("://", 1)[-1].split(":", 1)[0]
        return f"postgresql://{user}:***@{tail}"
    except Exception:
        return "postgresql://…"


def _load_join_metadata(repo_root: Path, dataset_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Optional `eval/join_metadata/<dataset>.json` with { \"joins\": [...] }."""
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in dataset_id.strip())
    for rel in (Path("eval") / "join_metadata" / f"{safe}.json", Path("artifacts") / "schema_registry" / "joins" / f"{safe}.json"):
        p = repo_root / rel
        if p.is_file():
            try:
                return json.loads(p.read_text(encoding="utf-8")), str(p.relative_to(repo_root))
            except Exception:
                return None, str(rel)
    return None, None


def _config_sources(repo_root: Path) -> Dict[str, str]:
    cfg = os.getenv("ORACLE_FORGE_DATASETS_CONFIG", "").strip()
    path = Path(cfg) if cfg else repo_root / "eval" / "datasets.json"
    return {"datasets_config": str(path.relative_to(repo_root)) if path.is_file() else str(path)}


def _registry_mongodb_database_name(
    repo_root: Path, dataset_id: str, prof: Optional[DatasetProfile]
) -> str:
    """
    Resolve the Mongo database name for registry builds.

    If ``eval/datasets.json`` (or ``ORACLE_FORGE_DATASETS_CONFIG``) contains an explicit
    ``mongodb_database`` key for the dataset, use its value (empty string means skip).

    If the key is **absent**, do not invent ``{dataset}_db`` and do not fall back to
    global ``MONGODB_DATABASE`` (that produced empty collections for SQL-only benchmarks).
    Set ``ORACLE_FORGE_SCHEMA_REGISTRY_MONGODB_ENV_FALLBACK=1`` to restore legacy
    ``mongodb_database_name(profile)`` behavior.
    """
    cfg = os.getenv("ORACLE_FORGE_DATASETS_CONFIG", "").strip()
    path = Path(cfg) if cfg else repo_root / "eval" / "datasets.json"
    did = dataset_id.strip()
    explicit_present = False
    explicit_value = ""
    if path.is_file():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            block = (data.get("datasets") or {}).get(did) or (data.get("datasets") or {}).get(did.lower())
            if isinstance(block, dict) and "mongodb_database" in block:
                explicit_present = True
                explicit_value = (block.get("mongodb_database") or "").strip()
        except Exception:
            pass
    if explicit_present:
        return explicit_value
    if (os.getenv("ORACLE_FORGE_SCHEMA_REGISTRY_MONGODB_ENV_FALLBACK") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return mongodb_database_name(prof.mongodb_database if prof else None)
    return ""


def build_schema_registry(
    dataset_id: str,
    *,
    repo_root: Optional[Path] = None,
    profile: Optional[DatasetProfile] = None,
    output_path: Optional[Path] = None,
    question: str = "",
    log: bool = True,
    strict: bool = False,
    persist: bool = True,
) -> Tuple[Dict[str, Any], Path]:
    """
    Introspect all engines reachable from env + dataset profile; write registry JSON.

    Returns (registry_dict, written_path).
    Raises RuntimeError when ``strict`` and no tables/collections were captured.
    """
    root = repo_root or Path(__file__).resolve().parents[2]
    load_registry_environment(root)
    prof = profile or load_dataset_profile(dataset_id, repo_root=root)
    if prof is not None:
        disc = discover_dab_connection_paths(root, dataset_id)
        if not prof.sqlite_path and disc.get("sqlite_path"):
            prof.sqlite_path = disc["sqlite_path"]
        if not prof.duckdb_path and disc.get("duckdb_path"):
            prof.duckdb_path = disc["duckdb_path"]
    out_path = output_path or default_registry_path(dataset_id, root)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    attempt = 1
    join_blob, join_ref = _load_join_metadata(root, dataset_id)

    sources = _config_sources(root)
    if join_ref:
        sources["join_metadata"] = join_ref

    pg_dsn = resolved_postgres_dsn()
    if prof and getattr(prof, "postgres_dsn", None):
        pg_dsn = (prof.postgres_dsn or "").strip() or pg_dsn

    mongo_uri = resolved_mongodb_uri()
    mongo_db = _registry_mongodb_database_name(root, dataset_id, prof)

    sqlite_path: Optional[Path] = None
    if prof and prof.sqlite_path:
        sqlite_path = Path(prof.sqlite_path)
        if not sqlite_path.is_absolute():
            sqlite_path = (root / sqlite_path).resolve()

    duck_path: Optional[Path] = None
    if prof and prof.duckdb_path:
        duck_path = Path(prof.duckdb_path)
        if not duck_path.is_absolute():
            duck_path = (root / duck_path).resolve()

    engines: Dict[str, Any] = {}
    provenance: List[Dict[str, Any]] = []

    # SQLite
    if sqlite_path and sqlite_path.is_file():
        eng, prov = introspect_sqlite(sqlite_path)
        engines["sqlite"] = eng
        provenance.append(prov)
    else:
        engines["sqlite"] = {
            "available": False,
            "engine": "sqlite",
            "skipped_reason": "sqlite_path_missing_or_unreadable",
            "tables": [],
            "collections": [],
        }

    # DuckDB
    if duck_path and duck_path.is_file():
        eng, prov = introspect_duckdb(duck_path)
        engines["duckdb"] = eng
        provenance.append(prov)
    else:
        engines["duckdb"] = {
            "available": False,
            "engine": "duckdb",
            "skipped_reason": "duckdb_path_missing_or_unreadable",
            "tables": [],
            "collections": [],
        }

    # PostgreSQL: prefer asyncpg; fall back to psycopg sync (see requirements.txt).
    if pg_dsn:
        try:
            import asyncpg  # noqa: F401

            eng, prov = asyncio.run(run_postgres(pg_dsn))
            prov = dict(prov)
            prov["dsn"] = _redact_dsn(pg_dsn)
            engines["postgresql"] = eng
            provenance.append(prov)
        except ImportError:
            eng, prov = introspect_postgresql_sync(pg_dsn)
            prov = dict(prov)
            prov["dsn"] = _redact_dsn(pg_dsn)
            engines["postgresql"] = eng
            provenance.append(prov)
    else:
        engines["postgresql"] = {
            "available": False,
            "engine": "postgresql",
            "skipped_reason": "POSTGRES_DSN_not_set",
            "tables": [],
            "collections": [],
        }

    # MongoDB
    if mongo_uri and mongo_db:
        eng, prov = introspect_mongodb(mongo_uri, mongo_db)
        engines["mongodb"] = eng
        provenance.append(prov)
    elif mongo_uri:
        engines["mongodb"] = {
            "available": False,
            "engine": "mongodb",
            "skipped_reason": "mongodb_database_not_configured_in_eval_datasets_json",
            "tables": [],
            "collections": [],
        }
    else:
        engines["mongodb"] = {
            "available": False,
            "engine": "mongodb",
            "skipped_reason": "MONGODB_URI_missing",
            "tables": [],
            "collections": [],
        }

    verified_joins: List[Any] = []
    if join_blob and isinstance(join_blob.get("joins"), list):
        verified_joins = list(join_blob["joins"])

    registry: Dict[str, Any] = {
        "schema_registry_version": SCHEMA_REGISTRY_VERSION,
        "dataset_id": dataset_id.strip(),
        "built_at_utc": _utc_now(),
        "sources": sources,
        "verified_joins": verified_joins,
        "engines": engines,
        "provenance": provenance,
        "freshness": {"registry_built_at_utc": _utc_now(), "per_engine": provenance},
    }

    enrich_registry_intent_summaries(registry)

    total_objects = 0
    for eng in engines.values():
        if not isinstance(eng, dict):
            continue
        total_objects += len(eng.get("tables") or [])
        total_objects += len(eng.get("collections") or [])

    duration_ms = int((time.perf_counter() - t0) * 1000)
    status = "ok" if total_objects > 0 else "empty"
    err_msg: Optional[str] = None
    if strict and total_objects == 0:
        err_msg = "strict_mode:no_tables_or_collections_introspected"
        status = "error"

    if persist:
        out_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8")

    if log:
        log_schema_registry_event(
            {
                "timestamp": _utc_now(),
                "dataset_id": dataset_id.strip(),
                "question": question or "(schema_registry_build)",
                "phase": "schema_registry",
                "selected_engine_s": [k for k, v in engines.items() if isinstance(v, dict) and v.get("available")],
                "selected_tables": [],
                "input_artifact_refs": [sources.get("datasets_config", ""), join_ref or ""],
                "output_artifact_refs": [str(out_path.relative_to(root))],
                "status": status,
                "error": err_msg,
                "warning": None if total_objects else "zero_objects_introspected_check_paths_and_env",
                "attempt_number": attempt,
                "duration_ms": duration_ms,
            },
            repo_root=root,
        )

    if strict and total_objects == 0:
        raise RuntimeError(err_msg or "strict_mode:no_tables_or_collections_introspected")

    return registry, out_path
