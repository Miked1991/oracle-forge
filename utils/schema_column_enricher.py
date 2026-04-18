"""Populate per-table/collection `fields` in schema_metadata via live introspection (DURABLE_FIX)."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

_logger = logging.getLogger(__name__)


def _canonical_db(name: str) -> str:
    text = (name or "").strip().lower()
    if "post" in text:
        return "postgresql"
    if "mongo" in text:
        return "mongodb"
    if "duck" in text:
        return "duckdb"
    if "sqlite" in text:
        return "sqlite"
    return text


def _table_fields_from_mapping(mapping: Dict[str, str]) -> Dict[str, str]:
    return {str(k): str(v) for k, v in mapping.items()}


async def _postgres_columns(dsn: str, table: str) -> Dict[str, str]:
    """Resolve ``table`` case-insensitively in ``public``, then load column types."""
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        nrow = await conn.fetchrow(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND lower(table_name) = lower($1)
            LIMIT 1
            """,
            table,
        )
        resolved = str(nrow["table_name"]) if nrow else table
        rows = await conn.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
            """,
            resolved,
        )
        return {str(r["column_name"]): str(r["data_type"]) for r in rows}
    finally:
        await conn.close()


def _sqlite_columns(path: Path, table: str) -> Dict[str, str]:
    import sqlite3

    con = sqlite3.connect(str(path))
    try:
        cur = con.cursor()
        cur.execute(f'PRAGMA table_info("{table}")')
        out: Dict[str, str] = {}
        for r in cur.fetchall():
            # cid, name, type, notnull, dflt, pk
            out[str(r[1])] = str(r[2] or "")
        return out
    finally:
        con.close()


def _duckdb_enrich_timeout_sec() -> Optional[float]:
    """Seconds per table for DuckDB introspection; ``None`` = no timeout (can hang on bad paths)."""
    raw = os.getenv("ORACLE_FORGE_DUCKDB_ENRICH_TIMEOUT_SEC", "25").strip()
    if raw.lower() in {"", "0", "none", "off", "false"}:
        return None
    try:
        return max(0.5, float(raw))
    except ValueError:
        return 25.0


def _duckdb_columns(path: Path, table: str) -> Dict[str, str]:
    import duckdb

    ident = f'"{table}"' if table.lower() == "user" else table
    con = None
    try:
        con = duckdb.connect(str(path), read_only=True)
        rows = con.execute(f"DESCRIBE {ident}").fetchall()
        out: Dict[str, str] = {}
        for r in rows:
            # column_name, column_type, ...
            out[str(r[0])] = str(r[1] if len(r) > 1 else "")
        return out
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _duckdb_columns_maybe_timed(path: Path, table: str) -> Dict[str, str]:
    """
    Run DuckDB introspection with optional timeout.

    Uses a daemon thread + ``join(timeout=…)`` so we do not block forever on ``duckdb.connect``.
    If the call times out, the worker thread may still be running in the background (daemon);
    prefer ``ORACLE_FORGE_DUCKDB_SCHEMA_ENRICH=false`` if the file path is unusable.
    """
    timeout = _duckdb_enrich_timeout_sec()
    if timeout is None:
        return _duckdb_columns(path, table)

    box: List[Any] = []

    def _target() -> None:
        try:
            box.append(_duckdb_columns(path, table))
        except BaseException as exc:  # noqa: BLE001 — propagate from worker
            box.append(exc)

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        _logger.warning(
            "duckdb schema enrich timed out after %ss for table %r (path=%s); skipping. "
            "Set ORACLE_FORGE_DUCKDB_SCHEMA_ENRICH=false or fix DUCKDB_PATH.",
            timeout,
            table,
            path,
        )
        return {}
    if not box:
        return {}
    if isinstance(box[0], BaseException):
        raise box[0]
    return box[0] if isinstance(box[0], dict) else {}


def _mongo_collection_fields(uri: str, database: str, collection: str, sample_limit: int = 50) -> Dict[str, str]:
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    try:
        client.admin.command("ping")
        keys: set[str] = set()
        for doc in client[database][collection].find().limit(sample_limit):
            if not isinstance(doc, dict):
                continue
            for k in doc.keys():
                if k != "_id":
                    keys.add(str(k))
                else:
                    keys.add("_id")
        return {k: "mixed" for k in sorted(keys)}
    finally:
        client.close()


def _needs_fields(item: Any) -> bool:
    if not isinstance(item, dict):
        return True
    fields = item.get("fields")
    if not isinstance(fields, dict) or len(fields) == 0:
        return True
    return False


def _merge_item_fields(item: Dict[str, Any], fields: Dict[str, str]) -> Dict[str, Any]:
    out = dict(item)
    out["fields"] = _table_fields_from_mapping(fields)
    return out


def enrich_schema_metadata_columns(
    schema_metadata: Dict[str, Any],
    selected_databases: List[str],
    *,
    repo_root: Optional[Path] = None,
    postgres_dsn: Optional[str] = None,
    sqlite_path: Optional[str] = None,
    duckdb_path: Optional[str] = None,
    mongo_uri: Optional[str] = None,
    mongo_database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Deep-copy schema_metadata and fill empty `fields` maps by introspecting live databases.
    No-op for engines that cannot be reached or paths missing.
    """
    if os.getenv("ORACLE_FORGE_ENRICH_SCHEMA_COLUMNS", "true").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return schema_metadata

    meta = copy.deepcopy(schema_metadata)
    selected = [_canonical_db(d) for d in selected_databases if _canonical_db(str(d))]
    root = repo_root or Path(__file__).resolve().parents[1]

    pg_dsn = (postgres_dsn or "").strip()
    sql_path = Path(sqlite_path) if sqlite_path else None
    if sql_path and not sql_path.is_absolute():
        sql_path = (root / sql_path).resolve()
    duck_path = Path(duckdb_path) if duckdb_path else None
    if duck_path and not duck_path.is_absolute():
        duck_path = (root / duck_path).resolve()
    muri = (mongo_uri or "").strip()
    mdb = (mongo_database or os.getenv("MONGODB_DATABASE") or "yelp_db").strip()

    async def _run_pg() -> None:
        if not pg_dsn or "postgresql" not in selected:
            return
        dbmeta = meta.get("postgresql") or {}
        tables = dbmeta.get("tables") or []
        for i, item in enumerate(list(tables)):
            if not isinstance(item, dict):
                continue
            tname = str(item.get("name", "")).strip()
            if not tname or not _needs_fields(item):
                continue
            try:
                cols = await _postgres_columns(pg_dsn, tname)
                if cols:
                    tables[i] = _merge_item_fields(item, cols)
            except Exception as exc:
                if os.getenv("ORACLE_FORGE_DEBUG_SCHEMA_ENRICH", "").strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }:
                    _logger.warning("postgresql column enrich failed for %r: %s", tname, exc)
                continue
        meta.setdefault("postgresql", {})["tables"] = tables

    def _run_sqlite() -> None:
        if not sql_path or not sql_path.exists() or "sqlite" not in selected:
            return
        dbmeta = meta.get("sqlite") or {}
        tables = dbmeta.get("tables") or []
        for i, item in enumerate(list(tables)):
            if not isinstance(item, dict):
                continue
            tname = str(item.get("name", "")).strip()
            if not tname or not _needs_fields(item):
                continue
            try:
                cols = _sqlite_columns(sql_path, tname)
                if cols:
                    tables[i] = _merge_item_fields(item, cols)
            except Exception:
                continue
        meta.setdefault("sqlite", {})["tables"] = tables

    def _run_duck() -> None:
        if os.getenv("ORACLE_FORGE_DUCKDB_SCHEMA_ENRICH", "true").strip().lower() not in {
            "1",
            "true",
            "yes",
            "on",
        }:
            return
        if not duck_path or not duck_path.exists() or "duckdb" not in selected:
            return
        dbmeta = meta.get("duckdb") or {}
        tables = dbmeta.get("tables") or []
        for i, item in enumerate(list(tables)):
            if not isinstance(item, dict):
                continue
            tname = str(item.get("name", "")).strip()
            if not tname or not _needs_fields(item):
                continue
            try:
                cols = _duckdb_columns_maybe_timed(duck_path, tname)
                if cols:
                    tables[i] = _merge_item_fields(item, cols)
            except Exception:
                continue
        meta.setdefault("duckdb", {})["tables"] = tables

    def _run_mongo() -> None:
        if not muri or "mongodb" not in selected:
            return
        dbmeta = meta.get("mongodb") or {}
        cols_list = dbmeta.get("collections") or []
        for i, item in enumerate(list(cols_list)):
            if not isinstance(item, dict):
                continue
            cname = str(item.get("name", "")).strip()
            if not cname or not _needs_fields(item):
                continue
            try:
                fields = _mongo_collection_fields(muri, mdb, cname)
                if fields:
                    cols_list[i] = _merge_item_fields(item, fields)
            except Exception:
                continue
        meta.setdefault("mongodb", {})["collections"] = cols_list

    asyncio.run(_run_pg())
    _run_sqlite()
    _run_duck()
    _run_mongo()
    return meta


def rebuild_schema_bundle_context(
    context: Dict[str, Any],
    available_databases: List[str],
    dataset_id: Optional[str],
    repo_root: Optional[Path] = None,
) -> None:
    """Update context schema_bundle and JSON after metadata mutation (Phase 4: table-scoped when routing provides tables)."""
    from pathlib import Path as P

    from utils.scoped_schema_pack import rebuild_with_scoped_pack

    root = repo_root or P(__file__).resolve().parents[1]
    rebuild_with_scoped_pack(context, available_databases, dataset_id, repo_root=root)
