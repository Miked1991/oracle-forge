"""Phase 1: schema registry introspection and build."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts.build_schema_registry import _dataset_ids_from_config
from utils.dataset_profiles import DatasetProfile
from utils.schema_registry.env import resolved_postgres_dsn
from utils.schema_registry.builder import (
    SCHEMA_REGISTRY_VERSION,
    _registry_mongodb_database_name,
    build_schema_registry,
)
from utils.schema_registry.intent_summaries import enrich_registry_intent_summaries
from utils.schema_registry.introspect import introspect_duckdb, introspect_sqlite


def test_enrich_registry_intent_summaries_fills_tables() -> None:
    reg = {
        "dataset_id": "test_ds",
        "engines": {
            "sqlite": {
                "available": True,
                "engine": "sqlite",
                "tables": [
                    {
                        "name": "t1",
                        "kind": "table",
                        "columns": [{"name": "id", "data_type": "INTEGER"}],
                        "primary_key": ["id"],
                        "foreign_keys": [],
                        "intent_summary": "",
                        "intent_summary_pending": True,
                        "row_count_estimate": 42,
                    }
                ],
                "collections": [],
            }
        },
    }
    enrich_registry_intent_summaries(reg)
    t0 = reg["engines"]["sqlite"]["tables"][0]
    assert t0["intent_summary_pending"] is False
    assert "test_ds" in t0["intent_summary"]
    assert "42" in t0["intent_summary"]
    assert "dataset_intent_summary" in reg


def test_resolved_postgres_dsn_from_split_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("ORACLE_FORGE_REGISTRY_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_HOST", "db.example")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "u")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p")
    monkeypatch.setenv("POSTGRES_DB", "appdb")
    dsn = resolved_postgres_dsn()
    assert "db.example:5433" in dsn
    assert "appdb" in dsn


def test_registry_mongodb_database_requires_explicit_datasets_json_key(tmp_path: Path) -> None:
    """Without ``mongodb_database`` in config, registry must not fall back to global MONGODB_DATABASE."""
    cfg = tmp_path / "eval" / "datasets.json"
    cfg.parent.mkdir(parents=True)
    cfg.write_text(
        json.dumps({"datasets": {"nodemongo": {"sqlite_path": "x.db"}}}),
        encoding="utf-8",
    )
    prof = DatasetProfile(dataset_id="nodemongo", sqlite_path="/tmp/x.db")
    assert _registry_mongodb_database_name(tmp_path, "nodemongo", prof) == ""


def test_registry_mongodb_database_reads_explicit_key(tmp_path: Path) -> None:
    cfg = tmp_path / "eval" / "datasets.json"
    cfg.parent.mkdir(parents=True)
    cfg.write_text(
        json.dumps({"datasets": {"hasmongo": {"mongodb_database": "my_db"}}}),
        encoding="utf-8",
    )
    assert _registry_mongodb_database_name(tmp_path, "hasmongo", None) == "my_db"


def test_all_datasets_cli_covers_eval_datasets_json() -> None:
    """``--all-datasets`` must enumerate every key under datasets (excluding comment)."""
    root = Path(__file__).resolve().parents[1]
    data = json.loads((root / "eval" / "datasets.json").read_text(encoding="utf-8"))
    expected = sorted(k for k in (data.get("datasets") or {}) if isinstance(data["datasets"].get(k), dict))
    assert _dataset_ids_from_config(root) == expected


def _make_sqlite(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
        conn.execute("INSERT INTO foo (id, label) VALUES (1, 'a')")
        conn.commit()
    finally:
        conn.close()


def _make_duckdb(path: Path) -> None:
    import duckdb

    con = duckdb.connect(str(path))
    try:
        con.execute("CREATE TABLE bar (id INTEGER PRIMARY KEY, score DOUBLE);")
        con.execute("INSERT INTO bar VALUES (1, 3.14);")
    finally:
        con.close()


def test_introspect_sqlite_columns_and_pk(tmp_path: Path) -> None:
    db = tmp_path / "t.sqlite"
    _make_sqlite(db)
    eng, prov = introspect_sqlite(db)
    assert eng["available"] is True
    assert prov["status"] == "ok"
    tables = {t["name"]: t for t in eng["tables"]}
    assert "foo" in tables
    assert [c["name"] for c in tables["foo"]["columns"]] == ["id", "label"]
    assert tables["foo"]["primary_key"] == ["id"]


def test_introspect_duckdb_table(tmp_path: Path) -> None:
    db = tmp_path / "t.duckdb"
    _make_duckdb(db)
    eng, prov = introspect_duckdb(db)
    assert eng["available"] is True
    assert prov["status"] == "ok"
    tables = {t["name"]: t for t in eng["tables"]}
    assert "bar" in tables
    assert tables["bar"]["primary_key"] == ["id"]
    assert tables["bar"]["columns"][0]["is_primary_key"] is True


def test_build_registry_sqlite_duckdb_only(tmp_path: Path) -> None:
    sql = tmp_path / "a.sqlite"
    duck = tmp_path / "b.duckdb"
    _make_sqlite(sql)
    _make_duckdb(duck)

    (tmp_path / "eval" / "join_metadata").mkdir(parents=True)
    (tmp_path / "eval" / "join_metadata" / "reg1.json").write_text(
        json.dumps({"joins": [{"left": "foo", "right": "bar", "verified": True}]}),
        encoding="utf-8",
    )

    profile = DatasetProfile(
        dataset_id="reg1",
        sqlite_path=str(sql.resolve()),
        duckdb_path=str(duck.resolve()),
    )
    reg, out_path = build_schema_registry(
        "reg1",
        repo_root=tmp_path,
        profile=profile,
        log=False,
        persist=True,
    )
    assert reg["schema_registry_version"] == SCHEMA_REGISTRY_VERSION
    assert reg["verified_joins"] == [{"left": "foo", "right": "bar", "verified": True}]
    assert reg["engines"]["sqlite"]["available"] is True
    assert reg["engines"]["duckdb"]["available"] is True
    assert "foo" in {t["name"] for t in reg["engines"]["sqlite"]["tables"]}
    assert out_path.is_file()
    loaded = json.loads(out_path.read_text(encoding="utf-8"))
    assert loaded["dataset_id"] == "reg1"


@pytest.mark.skipif(
    not Path("eval/datasets.json").is_file(),
    reason="eval/datasets.json missing",
)
def test_yelp_paths_resolve_when_dab_present() -> None:
    """Representative dataset: assert bundled eval config maps to files when DAB dbs exist."""
    from utils.dataset_profiles import load_dataset_profile

    prof = load_dataset_profile("yelp")
    if prof is None:
        pytest.skip("no dataset profile env/paths")
    root = Path(__file__).resolve().parents[1]
    if prof.sqlite_path and not Path(prof.sqlite_path).is_file():
        pytest.skip("yelp sqlite snapshot not present")
    if prof.duckdb_path and not Path(prof.duckdb_path).is_file():
        pytest.skip("yelp duckdb snapshot not present")
    reg, _ = build_schema_registry("yelp", repo_root=root, log=False, persist=False)
    assert reg["engines"]["sqlite"].get("available") or reg["engines"]["duckdb"].get("available")
