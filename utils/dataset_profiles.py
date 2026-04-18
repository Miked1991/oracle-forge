"""Dataset connection profiles (DURABLE_FIX_PLAN Phase C)."""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

@dataclass
class DatasetProfile:
    """Optional overrides for SQLite/DuckDB paths and Mongo database name."""

    dataset_id: str
    mongodb_database: Optional[str] = None
    sqlite_path: Optional[str] = None
    duckdb_path: Optional[str] = None
    postgres_dsn: Optional[str] = None

    def env_overrides(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if self.mongodb_database:
            out["MONGODB_DATABASE"] = self.mongodb_database
        if self.sqlite_path:
            out["SQLITE_PATH"] = self.sqlite_path
        if self.duckdb_path:
            out["DUCKDB_PATH"] = self.duckdb_path
        if self.postgres_dsn:
            out["POSTGRES_DSN"] = self.postgres_dsn
        return out


def _safe_key(name: str) -> str:
    return re.sub(r"[^A-Z0-9_]", "_", name.upper())


def _default_mongo_db_name(dataset_id: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "_", dataset_id.strip()).strip("_").lower()
    return f"{base}_db" if base else "default_db"


def _find_query_dataset_dir(repo_root: Path, dataset_id: str) -> Optional[Path]:
    """Return ``.../DataAgentBench/query_<dataset>/query_dataset`` if present."""
    dab = repo_root / "DataAgentBench"
    if not dab.is_dir():
        return None
    folder = f"query_{dataset_id.strip()}"
    direct = dab / folder
    if direct.is_dir():
        qd = direct / "query_dataset"
        if qd.is_dir():
            return qd
    for child in dab.iterdir():
        if child.is_dir() and child.name.lower() == folder.lower():
            qd = child / "query_dataset"
            if qd.is_dir():
                return qd
    return None


def discover_dab_connection_paths(repo_root: Path, dataset_id: str) -> Dict[str, str]:
    """
    Scan ``DataAgentBench/query_<id>/query_dataset/*.db`` and map files to env-style paths.

    Convention (matches bundled ``eval/datasets.json``): ``*_user.db`` → DuckDB analytics,
    ``*_mongo.db`` → SQLite snapshot; remaining ``*.db`` fill missing slots.
    """
    out: Dict[str, str] = {}
    qd = _find_query_dataset_dir(repo_root, dataset_id)
    if not qd:
        return out
    dbs = sorted(qd.glob("*.db"))
    if not dbs:
        return out
    user_dbs = [p for p in dbs if "user" in p.name.lower()]
    mongo_named = [p for p in dbs if "mongo" in p.name.lower()]
    tagged = set(user_dbs) | set(mongo_named)
    others = [p for p in dbs if p not in tagged]

    if user_dbs:
        out["duckdb_path"] = str(user_dbs[0].resolve())
    if mongo_named:
        out["sqlite_path"] = str(mongo_named[0].resolve())
    if "duckdb_path" not in out and others:
        out["duckdb_path"] = str(others[0].resolve())
        others = others[1:]
    if "sqlite_path" not in out and others:
        out["sqlite_path"] = str(others[0].resolve())
    return out


def _merge_env_into_profile(dataset_id: str, base: DatasetProfile) -> DatasetProfile:
    """Apply ORACLE_FORGE_DATASET_<ID>_* environment variables."""
    prefix = f"ORACLE_FORGE_DATASET_{_safe_key(dataset_id)}_"
    mapping = {
        f"{prefix}MONGODB_DATABASE": "mongodb_database",
        f"{prefix}SQLITE_PATH": "sqlite_path",
        f"{prefix}DUCKDB_PATH": "duckdb_path",
        f"{prefix}POSTGRES_DSN": "postgres_dsn",
    }
    for env_key, attr in mapping.items():
        val = os.getenv(env_key, "").strip()
        if val:
            setattr(base, attr, val)
    return base


def load_dataset_profile(
    dataset_id: Optional[str],
    repo_root: Optional[Path] = None,
) -> Optional[DatasetProfile]:
    if not dataset_id or not str(dataset_id).strip():
        return None
    did = str(dataset_id).strip()
    repo_root = repo_root or Path(__file__).resolve().parents[1]
    cfg_path = os.getenv("ORACLE_FORGE_DATASETS_CONFIG", "").strip()
    path = Path(cfg_path) if cfg_path else repo_root / "eval" / "datasets.json"
    profile = DatasetProfile(dataset_id=did)
    block: Optional[Dict[str, Any]] = None
    if path.is_file():
        try:
            import json

            data = json.loads(path.read_text(encoding="utf-8"))
            datasets = data.get("datasets") or {}
            block = datasets.get(did) or datasets.get(did.lower())
            if isinstance(block, dict):
                profile.mongodb_database = (block.get("mongodb_database") or block.get("mongo_database") or "").strip() or None
                raw_sqlite = (block.get("sqlite_path") or "").strip()
                raw_duck = (block.get("duckdb_path") or "").strip()
                profile.sqlite_path = str((repo_root / raw_sqlite).resolve()) if raw_sqlite and not Path(raw_sqlite).is_absolute() else (raw_sqlite or None)
                profile.duckdb_path = str((repo_root / raw_duck).resolve()) if raw_duck and not Path(raw_duck).is_absolute() else (raw_duck or None)
                profile.postgres_dsn = (block.get("postgres_dsn") or "").strip() or None
        except Exception:
            pass
    profile = _merge_env_into_profile(did, profile)

    if profile.duckdb_path and not Path(profile.duckdb_path).is_file():
        profile.duckdb_path = None
    if profile.sqlite_path and not Path(profile.sqlite_path).is_file():
        profile.sqlite_path = None

    discovered = discover_dab_connection_paths(repo_root, did)
    if not profile.duckdb_path and discovered.get("duckdb_path"):
        profile.duckdb_path = discovered["duckdb_path"]
    if not profile.sqlite_path and discovered.get("sqlite_path"):
        profile.sqlite_path = discovered["sqlite_path"]

    has_path = bool(profile.duckdb_path or profile.sqlite_path or profile.postgres_dsn)
    if not profile.mongodb_database and (has_path or discovered):
        profile.mongodb_database = _default_mongo_db_name(did)

    if not profile.env_overrides():
        # Allow callers (e.g. schema registry) to still resolve Postgres/Mongo from process env
        # when the dataset block exists but no local *.db paths are present.
        if isinstance(block, dict):
            return profile
        return None
    return profile


def push_profile_env(profile: Optional[DatasetProfile]) -> Dict[str, Optional[str]]:
    """Apply profile env overrides; return saved values for pop_profile_env."""
    if not profile:
        return {}
    overrides = profile.env_overrides()
    saved: Dict[str, Optional[str]] = {k: os.environ.get(k) for k in overrides}
    for k, v in overrides.items():
        os.environ[k] = v
    return saved


def pop_profile_env(profile: Optional[DatasetProfile], saved: Dict[str, Optional[str]]) -> None:
    if not profile or not saved:
        return
    for k in profile.env_overrides():
        old = saved.get(k)
        if old is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = old  # type: ignore[assignment]


@contextmanager
def use_dataset_profile(profile: Optional[DatasetProfile]) -> Iterator[None]:
    """Temporarily set connection-related env vars for MCP tools that read process env."""
    saved = push_profile_env(profile)
    try:
        yield
    finally:
        pop_profile_env(profile, saved)
