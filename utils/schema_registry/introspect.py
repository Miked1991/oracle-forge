"""Live introspection for PostgreSQL, MongoDB, SQLite, DuckDB → registry-shaped dicts."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def introspect_sqlite(path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Returns (engine_block, provenance) where engine_block has tables[] with columns, pk, fks.
    """
    t0 = time.perf_counter()
    tables_out: List[Dict[str, Any]] = []
    err: Optional[str] = None
    try:
        conn = sqlite3.connect(str(path))
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            names = [r[0] for r in cur.fetchall()]
            for tname in names:
                cur.execute(f'PRAGMA table_info("{tname}")')
                cols_raw = cur.fetchall()
                # Last column is pk (0/1). Do not infer PK from NOT NULL (col 3); that mislabels constraints.
                pk_cols = [str(r[1]) for r in cols_raw if len(r) > 5 and int(r[5] or 0) != 0]

                columns: List[Dict[str, Any]] = []
                for r in cols_raw:
                    cid, name, ctype, notnull, _dflt, pk = r[0], r[1], r[2], r[3], r[4], r[5] if len(r) > 5 else 0
                    columns.append(
                        {
                            "name": str(name),
                            "data_type": str(ctype or ""),
                            "nullable": not bool(notnull),
                            "is_primary_key": int(pk or 0) != 0,
                        }
                    )

                fks: List[Dict[str, Any]] = []
                try:
                    cur.execute(f'PRAGMA foreign_key_list("{tname}")')
                    for fk in cur.fetchall():
                        # id, seq, table, from, to, on_update, on_delete, match
                        if len(fk) >= 4:
                            fks.append(
                                {
                                    "columns": [str(fk[3])],
                                    "referenced_table": str(fk[2]),
                                    "referenced_columns": [str(fk[4])] if len(fk) > 4 else [],
                                }
                            )
                except sqlite3.Error:
                    pass

                row_count = 0
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{tname}"')
                    row_count = int(cur.fetchone()[0])
                except sqlite3.Error:
                    pass

                tables_out.append(
                    {
                        "name": tname,
                        "kind": "table",
                        "columns": columns,
                        "primary_key": pk_cols,
                        "foreign_keys": fks,
                        "intent_summary": "",
                        "intent_summary_pending": True,
                        "row_count_estimate": row_count,
                    }
                )
        finally:
            conn.close()
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        el = err.lower()
        if "not a database" in el or "unable to open database file" in el:
            try:
                dblock, dprov = introspect_duckdb(path)
                dblock = dict(dblock)
                dblock["engine"] = "sqlite"
                dblock["native_storage_engine"] = "duckdb"
                dblock["note"] = (
                    "Path was listed as sqlite_path but file is not SQLite; schema was read with DuckDB."
                )
                dprov = dict(dprov)
                dprov["engine"] = "sqlite"
                dprov["resolved_via"] = "duckdb_reader"
                return dblock, dprov
            except Exception:
                pass

    duration_ms = int((time.perf_counter() - t0) * 1000)
    prov = {
        "engine": "sqlite",
        "path": str(path.resolve()),
        "introspected_at_utc": _utc_now(),
        "duration_ms": duration_ms,
        "status": "error" if err else "ok",
        "error": err,
        "table_count": len(tables_out),
    }
    block = {
        "available": err is None and len(tables_out) > 0,
        "engine": "sqlite",
        "tables": tables_out,
        "collections": [],
    }
    if err:
        block["error"] = err
    return block, prov


def _duck_quote_ident(name: str) -> str:
    """Quote DuckDB identifiers (reserved words, #, spaces)."""
    return '"' + str(name).replace('"', '""') + '"'


def _duck_table_name_literal(tname: str) -> str:
    """Single-quoted table name for ``PRAGMA table_info('...')``."""
    return "'" + str(tname).replace("'", "''") + "'"


def _duck_fetch_foreign_keys(con: Any, tname: str) -> List[Dict[str, Any]]:
    """Foreign keys from ``duckdb_constraints()`` (``PRAGMA foreign_key_list`` is not available)."""
    out: List[Dict[str, Any]] = []
    try:
        rows = con.execute(
            """
            SELECT constraint_column_names, referenced_table, referenced_column_names
            FROM duckdb_constraints()
            WHERE schema_name = 'main'
              AND table_name = ?
              AND constraint_type = 'FOREIGN KEY'
            """,
            [tname],
        ).fetchall()
    except Exception:
        return out
    for ccols, rtab, rcols in rows:
        cols = [str(x) for x in (ccols or [])]
        rcols_list = [str(x) for x in (rcols or [])]
        rtab_s = str(rtab or "")
        if not cols:
            continue
        if len(cols) == len(rcols_list) and cols:
            for i, col in enumerate(cols):
                out.append(
                    {
                        "columns": [col],
                        "referenced_table": rtab_s,
                        "referenced_columns": [rcols_list[i]] if i < len(rcols_list) else [],
                    }
                )
        else:
            out.append(
                {
                    "columns": cols,
                    "referenced_table": rtab_s,
                    "referenced_columns": rcols_list,
                }
            )
    return out


def introspect_duckdb(path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    import duckdb

    t0 = time.perf_counter()
    tables_out: List[Dict[str, Any]] = []
    err: Optional[str] = None
    try:
        con = duckdb.connect(str(path), read_only=True)
        try:
            rows = con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema IN ('main', 'temp')
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            ).fetchall()
            names = [str(r[0]) for r in rows]
            for tname in names:
                ident = _duck_quote_ident(tname)
                tbl_lit = _duck_table_name_literal(tname)
                ti = con.execute(f"PRAGMA table_info({tbl_lit})").fetchall()
                nullable_map: Dict[str, bool] = {}
                try:
                    for drow in con.execute(f"DESCRIBE {ident}").fetchall():
                        if len(drow) >= 3:
                            nullable_map[str(drow[0])] = str(drow[2]).upper() == "YES"
                except Exception:
                    pass

                pk_cols: List[str] = []
                columns: List[Dict[str, Any]] = []
                for row in ti:
                    if len(row) < 6:
                        continue
                    cname = str(row[1])
                    ctype = str(row[2] or "")
                    notnull_raw = row[3]
                    pk_raw = row[5]
                    if isinstance(pk_raw, bool):
                        is_pk = pk_raw
                    else:
                        is_pk = int(pk_raw or 0) != 0
                    if is_pk:
                        pk_cols.append(cname)
                    null = nullable_map.get(cname)
                    if null is None:
                        if isinstance(notnull_raw, bool):
                            null = not notnull_raw
                        else:
                            null = not bool(int(notnull_raw or 0))
                    columns.append(
                        {
                            "name": cname,
                            "data_type": ctype,
                            "nullable": null,
                            "is_primary_key": is_pk,
                        }
                    )

                fks = _duck_fetch_foreign_keys(con, tname)

                row_count = 0
                try:
                    row_count = int(con.execute(f"SELECT COUNT(*) FROM {ident}").fetchone()[0])
                except Exception:
                    pass

                tables_out.append(
                    {
                        "name": tname,
                        "kind": "table",
                        "columns": columns,
                        "primary_key": pk_cols,
                        "foreign_keys": fks,
                        "intent_summary": "",
                        "intent_summary_pending": True,
                        "row_count_estimate": row_count,
                    }
                )
        finally:
            con.close()
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"

    duration_ms = int((time.perf_counter() - t0) * 1000)
    prov = {
        "engine": "duckdb",
        "path": str(path.resolve()),
        "introspected_at_utc": _utc_now(),
        "duration_ms": duration_ms,
        "status": "error" if err else "ok",
        "error": err,
        "table_count": len(tables_out),
    }
    block = {
        "available": err is None and len(tables_out) > 0,
        "engine": "duckdb",
        "tables": tables_out,
        "collections": [],
    }
    if err:
        block["error"] = err
    return block, prov


async def introspect_postgresql(dsn: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    import asyncpg

    t0 = time.perf_counter()
    tables_out: List[Dict[str, Any]] = []
    err: Optional[str] = None
    try:
        conn = await asyncpg.connect(dsn)
        try:
            table_rows = await conn.fetch(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
            for tr in table_rows:
                tname = str(tr["table_name"])
                cols = await conn.fetch(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = $1
                    ORDER BY ordinal_position
                    """,
                    tname,
                )
                pk_rows = await conn.fetch(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = 'public'
                      AND tc.table_name = $1
                      AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                    """,
                    tname,
                )
                pk_cols = [str(r["column_name"]) for r in pk_rows]

                fk_rows = await conn.fetch(
                    """
                    SELECT
                      kcu.column_name AS col,
                      ccu.table_name AS ref_table,
                      ccu.column_name AS ref_col
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                     AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_schema = 'public'
                      AND tc.table_name = $1
                    """,
                    tname,
                )
                fks: List[Dict[str, Any]] = []
                for fk in fk_rows:
                    fks.append(
                        {
                            "columns": [str(fk["col"])],
                            "referenced_table": str(fk["ref_table"]),
                            "referenced_columns": [str(fk["ref_col"])],
                        }
                    )

                columns: List[Dict[str, Any]] = []
                for c in cols:
                    cname = str(c["column_name"])
                    columns.append(
                        {
                            "name": cname,
                            "data_type": str(c["data_type"]),
                            "nullable": str(c["is_nullable"]).upper() == "YES",
                            "is_primary_key": cname in pk_cols,
                        }
                    )

                row_count = 0
                try:
                    row_count = int(await conn.fetchval(f'SELECT COUNT(*) FROM "{tname}"'))
                except Exception:
                    pass

                tables_out.append(
                    {
                        "name": tname,
                        "kind": "table",
                        "columns": columns,
                        "primary_key": pk_cols,
                        "foreign_keys": fks,
                        "intent_summary": "",
                        "intent_summary_pending": True,
                        "row_count_estimate": row_count,
                    }
                )
        finally:
            await conn.close()
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"

    duration_ms = int((time.perf_counter() - t0) * 1000)
    prov = {
        "engine": "postgresql",
        "dsn": "postgresql://…",
        "introspected_at_utc": _utc_now(),
        "duration_ms": duration_ms,
        "status": "error" if err else "ok",
        "error": err,
        "table_count": len(tables_out),
    }
    block = {
        "available": err is None and len(tables_out) > 0,
        "engine": "postgresql",
        "tables": tables_out,
        "collections": [],
    }
    if err:
        block["error"] = err
    return block, prov


def introspect_mongodb(uri: str, database: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    from pymongo import MongoClient

    t0 = time.perf_counter()
    collections_out: List[Dict[str, Any]] = []
    err: Optional[str] = None
    try:
        timeout_ms = int(os.getenv("ORACLE_FORGE_REGISTRY_MONGO_TIMEOUT_MS", "30000"))
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=timeout_ms,
            connectTimeoutMS=min(timeout_ms, 60000),
        )
        try:
            client.admin.command("ping")
            db = client[database]
            for cname in sorted(db.list_collection_names()):
                if cname.startswith("system."):
                    continue
                sample = list(db[cname].find().limit(5))
                fields: Dict[str, str] = {}
                for doc in sample:
                    if not isinstance(doc, dict):
                        continue
                    for k, v in doc.items():
                        if k not in fields:
                            fields[k] = type(v).__name__ if v is not None else "null"
                row_count = 0
                try:
                    row_count = int(db[cname].count_documents({}))
                except Exception:
                    pass
                collections_out.append(
                    {
                        "name": cname,
                        "kind": "collection",
                        "columns": [{"name": k, "data_type": fields[k], "nullable": True, "is_primary_key": k == "_id"} for k in sorted(fields.keys())],
                        "primary_key": ["_id"] if "_id" in fields else [],
                        "foreign_keys": [],
                        "intent_summary": "",
                        "intent_summary_pending": True,
                        "row_count_estimate": row_count,
                    }
                )
        finally:
            client.close()
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"

    duration_ms = int((time.perf_counter() - t0) * 1000)
    prov = {
        "engine": "mongodb",
        "database": database,
        "introspected_at_utc": _utc_now(),
        "duration_ms": duration_ms,
        "status": "error" if err else "ok",
        "error": err,
        "collection_count": len(collections_out),
    }
    block = {
        "available": err is None,
        "engine": "mongodb",
        "tables": [],
        "collections": collections_out,
        "collections_empty": err is None and len(collections_out) == 0,
    }
    if err:
        block["error"] = err
    return block, prov


def introspect_postgresql_sync(dsn: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Same schema as async introspect_postgresql, using psycopg when asyncpg is unavailable."""
    try:
        import psycopg
        from psycopg import sql as psql
    except ImportError as exc:
        err = f"{type(exc).__name__}: {exc}"
        prov = {
            "engine": "postgresql",
            "dsn": "postgresql://…",
            "introspected_at_utc": _utc_now(),
            "duration_ms": 0,
            "status": "error",
            "error": err,
            "table_count": 0,
        }
        return (
            {
                "available": False,
                "engine": "postgresql",
                "skipped_reason": "missing_python_package_psycopg",
                "hint": "pip install 'psycopg[binary]>=3.2' or install asyncpg (see requirements.txt).",
                "tables": [],
                "collections": [],
                "error": err,
            },
            prov,
        )

    t0 = time.perf_counter()
    tables_out: List[Dict[str, Any]] = []
    err: Optional[str] = None
    try:
        with psycopg.connect(dsn, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                )
                table_names = [str(r[0]) for r in cur.fetchall()]
                for tname in table_names:
                    cur.execute(
                        """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s
                        ORDER BY ordinal_position
                        """,
                        (tname,),
                    )
                    cols = cur.fetchall()
                    cur.execute(
                        """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.table_schema = 'public'
                          AND tc.table_name = %s
                          AND tc.constraint_type = 'PRIMARY KEY'
                        ORDER BY kcu.ordinal_position
                        """,
                        (tname,),
                    )
                    pk_cols = [str(r[0]) for r in cur.fetchall()]
                    cur.execute(
                        """
                        SELECT kcu.column_name AS col,
                               ccu.table_name AS ref_table,
                               ccu.column_name AS ref_col
                        FROM information_schema.table_constraints AS tc
                        JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                          ON ccu.constraint_name = tc.constraint_name
                         AND ccu.table_schema = tc.table_schema
                        WHERE tc.constraint_type = 'FOREIGN KEY'
                          AND tc.table_schema = 'public'
                          AND tc.table_name = %s
                        """,
                        (tname,),
                    )
                    fks: List[Dict[str, Any]] = []
                    for fk in cur.fetchall():
                        fks.append(
                            {
                                "columns": [str(fk[0])],
                                "referenced_table": str(fk[1]),
                                "referenced_columns": [str(fk[2])],
                            }
                        )
                    columns: List[Dict[str, Any]] = []
                    for c in cols:
                        cname = str(c[0])
                        columns.append(
                            {
                                "name": cname,
                                "data_type": str(c[1]),
                                "nullable": str(c[2]).upper() == "YES",
                                "is_primary_key": cname in pk_cols,
                            }
                        )
                    row_count = 0
                    try:
                        cur.execute(psql.SQL("SELECT COUNT(*) FROM {}").format(psql.Identifier(tname)))
                        row_count = int(cur.fetchone()[0])
                    except Exception:
                        pass
                    tables_out.append(
                        {
                            "name": tname,
                            "kind": "table",
                            "columns": columns,
                            "primary_key": pk_cols,
                            "foreign_keys": fks,
                            "intent_summary": "",
                            "intent_summary_pending": True,
                            "row_count_estimate": row_count,
                        }
                    )
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"

    duration_ms = int((time.perf_counter() - t0) * 1000)
    prov = {
        "engine": "postgresql",
        "dsn": "postgresql://…",
        "introspected_at_utc": _utc_now(),
        "duration_ms": duration_ms,
        "status": "error" if err else "ok",
        "error": err,
        "table_count": len(tables_out),
        "driver": "psycopg",
    }
    block = {
        "available": err is None and len(tables_out) > 0,
        "engine": "postgresql",
        "tables": tables_out,
        "collections": [],
        "introspection_driver": "psycopg",
    }
    if err:
        block["error"] = err
    return block, prov


async def run_postgres(dsn: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return await introspect_postgresql(dsn)
