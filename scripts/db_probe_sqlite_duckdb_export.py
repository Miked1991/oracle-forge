"""
Write constraint + sample-row reports for SQLite/DuckDB files referenced by eval/datasets.json
and any *.db under database_export/. Output: database_export/sqlite_duckdb_probe/

Run from repo root: python scripts/db_probe_sqlite_duckdb_export.py
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_dataset_db_paths(root: Path) -> List[Tuple[str, str, Path]]:
    """(kind, dataset_id, path) — kind is sqlite_path or duckdb_path."""
    out: List[Tuple[str, str, Path]] = []
    cfg = root / "eval" / "datasets.json"
    if not cfg.is_file():
        return out
    data = json.loads(cfg.read_text(encoding="utf-8"))
    ds = data.get("datasets") or {}
    for did, block in ds.items():
        if did == "comment" or not isinstance(block, dict):
            continue
        for key in ("sqlite_path", "duckdb_path"):
            raw = (block.get(key) or "").strip()
            if not raw:
                continue
            p = Path(raw)
            if not p.is_absolute():
                p = (root / p).resolve()
            out.append((key, str(did), p))
    return out


def _find_export_dbs(root: Path) -> List[Path]:
    base = root / "database_export"
    if not base.is_dir():
        return []
    return sorted({p.resolve() for p in base.rglob("*.db")})


def _find_export_csvs(root: Path) -> List[Path]:
    base = root / "database_export"
    if not base.is_dir():
        return []
    return sorted({p.resolve() for p in base.rglob("*.csv")})


def _truncate(v: Any, n: int = 120) -> Any:
    if v is None:
        return None
    s = str(v)
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _sample_for_markdown(rows: List[Dict[str, Any]], *, cell_len: int = 72, max_rows: int = 1) -> List[Dict[str, Any]]:
    """Keep REPORT.md small: few rows, short cells (full data remains in report.json)."""
    out: List[Dict[str, Any]] = []
    for row in rows[:max_rows]:
        slim: Dict[str, Any] = {}
        for k, v in row.items():
            if k == "_error":
                slim[k] = v
            else:
                slim[str(k)] = _truncate(v, cell_len)
        out.append(slim)
    return out


def _sqlite_probe(path: Path) -> Dict[str, Any]:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [str(r[0]) for r in cur.fetchall()]
        blocks: List[Dict[str, Any]] = []
        for t in tables:
            cur.execute(f'PRAGMA table_info("{t}")')
            ti = [tuple(r) for r in cur.fetchall()]
            pk_cols = [str(r[1]) for r in ti if len(r) > 5 and int(r[5] or 0) != 0]
            cur.execute(f'PRAGMA foreign_key_list("{t}")')
            fks = [tuple(r) for r in cur.fetchall()]
            sample: List[Dict[str, Any]] = []
            try:
                cur.execute(f'SELECT * FROM "{t}" LIMIT 2')
                rows = cur.fetchall()
                for row in rows:
                    sample.append({k: _truncate(row[k]) for k in row.keys()})
            except sqlite3.Error as e:
                sample = [{"_error": str(e)}]
            blocks.append(
                {
                    "name": t,
                    "pragma_table_info": [
                        {
                            "cid": r[0],
                            "name": r[1],
                            "type": r[2],
                            "notnull": r[3],
                            "pk": r[5] if len(r) > 5 else 0,
                        }
                        for r in ti
                    ],
                    "primary_key_columns": pk_cols,
                    "foreign_key_list_rows": fks,
                    "sample_rows_limit_2": sample,
                }
            )
        return {"engine": "sqlite3", "tables": blocks}
    finally:
        conn.close()


def _duck_probe(path: Path) -> Dict[str, Any]:
    import duckdb

    con = duckdb.connect(str(path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema IN ('main', 'temp') AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
        names = [str(r[0]) for r in rows]
        blocks: List[Dict[str, Any]] = []
        for tname in names:
            esc = "'" + tname.replace("'", "''") + "'"
            ti = con.execute(f"PRAGMA table_info({esc})").fetchall()
            pk_cols = []
            for r in ti:
                if len(r) < 6:
                    continue
                pk_raw = r[5]
                if isinstance(pk_raw, bool):
                    is_pk = pk_raw
                else:
                    is_pk = int(pk_raw or 0) != 0
                if is_pk:
                    pk_cols.append(str(r[1]))
            try:
                cons = con.execute(
                    """
                    SELECT constraint_type, constraint_column_names, referenced_table, referenced_column_names
                    FROM duckdb_constraints()
                    WHERE schema_name = 'main' AND table_name = ?
                    """,
                    [tname],
                ).fetchall()
            except Exception as exc:
                cons = [("error", str(exc), None, None)]
            ident = '"' + tname.replace('"', '""') + '"'
            sample: List[Dict[str, Any]] = []
            try:
                for row in con.execute(f"SELECT * FROM {ident} LIMIT 2").fetchall():
                    cols = [d[0] for d in con.description or []]
                    sample.append({cols[i]: _truncate(row[i]) for i in range(len(cols))})
            except Exception as e:
                sample = [{"_error": str(e)}]
            blocks.append(
                {
                    "name": tname,
                    "pragma_table_info": [
                        {
                            "cid": r[0],
                            "name": r[1],
                            "type": r[2],
                            "notnull": r[3],
                            "pk": r[5] if len(r) > 5 else None,
                        }
                        for r in ti
                    ],
                    "primary_key_columns": pk_cols,
                    "duckdb_constraints_rows": [
                        {
                            "constraint_type": str(c[0]) if c[0] is not None else None,
                            "constraint_column_names": c[1],
                            "referenced_table": c[2],
                            "referenced_column_names": c[3],
                        }
                        for c in cons
                    ],
                    "sample_rows_limit_2": sample,
                }
            )
        return {"engine": "duckdb", "tables": blocks}
    finally:
        con.close()


def _probe_file(path: Path) -> Tuple[str, Dict[str, Any]]:
    try:
        conn = sqlite3.connect(str(path))
        try:
            conn.execute("SELECT 1 FROM sqlite_master LIMIT 1")
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return "duckdb", _duck_probe(path)
    return "sqlite", _sqlite_probe(path)


def main() -> None:
    root = _repo_root()
    out_dir = root / "database_export" / "sqlite_duckdb_probe"
    out_dir.mkdir(parents=True, exist_ok=True)

    seen: set[Path] = set()
    reports: Dict[str, Any] = {
        "repo_root": str(root),
        "sources": [],
    }

    for key, did, p in _load_dataset_db_paths(root):
        if p in seen or not p.is_file():
            continue
        seen.add(p)
        kind, payload = _probe_file(p)
        try:
            rel_p = str(p.relative_to(root))
        except ValueError:
            rel_p = str(p)
        entry = {
            "source": "eval/datasets.json",
            "dataset_id": did,
            "config_key": key,
            "path": rel_p,
            "detected_engine": kind,
            "introspection": payload,
        }
        reports["sources"].append(entry)

    export_dbs = _find_export_dbs(root)
    for p in export_dbs:
        if p in seen:
            continue
        seen.add(p)
        kind, payload = _probe_file(p)
        try:
            rel_p = str(p.relative_to(root))
        except ValueError:
            rel_p = str(p)
        entry = {
            "source": "database_export",
            "path": rel_p,
            "detected_engine": kind,
            "introspection": payload,
        }
        reports["sources"].append(entry)

    csv_paths = _find_export_csvs(root)
    csv_inventory: List[Dict[str, Any]] = []
    for cp in csv_paths:
        try:
            rel = str(cp.relative_to(root))
        except ValueError:
            rel = str(cp)
        head = ""
        try:
            head = cp.read_text(encoding="utf-8", errors="replace").splitlines()[0][:500]
        except OSError:
            head = "(unreadable)"
        csv_inventory.append({"path": rel, "header_line": head})

    reports["database_export_csv_files"] = csv_inventory

    json_path = out_dir / "report.json"
    json_path.write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")

    MD_TABLE_CAP = 30

    lines: List[str] = [
        "# SQLite / DuckDB probe (constraints + sample rows)",
        "",
        f"Generated under `{out_dir.relative_to(root)}`. See `report.json` for full structure.",
        "",
        f"Markdown lists at most **{MD_TABLE_CAP}** tables per file (alphabetically) so huge stock-symbol",
        "splits do not create multi‑MB reports. All tables remain in `report.json`.",
        "",
    ]
    for src in reports["sources"]:
        title = src.get("dataset_id") or Path(src["path"]).stem
        fname = Path(src["path"]).name
        lines.append(f"## {title} — `{fname}`")
        lines.append("")
        lines.append(f"- **Path:** `{src['path']}`")
        lines.append(f"- **Config:** {src.get('config_key', 'database_export')}")
        lines.append(f"- **Opened as:** {src['detected_engine']}")
        intro = src["introspection"]
        all_tables = list(intro.get("tables") or [])
        all_tables.sort(key=lambda x: str(x.get("name") or ""))
        if len(all_tables) > MD_TABLE_CAP:
            lines.append(
                f"- **Tables in file:** {len(all_tables)} *(showing first {MD_TABLE_CAP} by name in Markdown; see `report.json` for all)*"
            )
            lines.append("")
        tbl_iter = all_tables[:MD_TABLE_CAP]
        for tbl in tbl_iter:
            lines.append("")
            lines.append(f"### Table `{tbl['name']}`")
            pk = tbl.get("primary_key_columns") or []
            lines.append(f"- **Primary key (from pragma):** {pk if pk else '*(none)*'}")
            fks = tbl.get("foreign_key_list_rows") or []
            dcons = tbl.get("duckdb_constraints_rows") or []
            if fks:
                lines.append(f"- **SQLite PRAGMA foreign_key_list:** `{fks}`")
            if dcons:
                fk_only = [r for r in dcons if str(r.get("constraint_type") or "") == "FOREIGN KEY"]
                lines.append(
                    f"- **DuckDB FK constraints:** {fk_only if fk_only else '*(none or only non-FK rows)*'}"
                )
            slim = _sample_for_markdown(tbl.get("sample_rows_limit_2") or [])
            lines.append("- **Sample (1 row, truncated for Markdown; see `report.json` for 2 rows):**")
            lines.append("")
            blob = json.dumps(slim, ensure_ascii=False, indent=2)
            if len(blob) > 4000:
                lines.append("*(sample omitted in Markdown — row too large; open `report.json`)*")
            else:
                lines.append("```json")
                lines.append(blob)
                lines.append("```")
        lines.append("")
        lines.append("---")
        lines.append("")

    md_path = out_dir / "REPORT.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    csv_lines = [
        "# CSV files under database_export/",
        "",
        "First line of each file (header / preview).",
        "",
    ]
    for item in csv_inventory:
        csv_lines.append(f"## `{item['path']}`")
        csv_lines.append("")
        csv_lines.append("```")
        csv_lines.append(item["header_line"])
        csv_lines.append("```")
        csv_lines.append("")
    (out_dir / "CSV_EXPORT_HEADERS.md").write_text("\n".join(csv_lines), encoding="utf-8")

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(f"Wrote {out_dir / 'CSV_EXPORT_HEADERS.md'}")


if __name__ == "__main__":
    main()
