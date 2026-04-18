"""Resolve DB connection settings for schema registry builds (host-side, Docker-friendly)."""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import dotenv_values, load_dotenv


def _set_if_blank_from_file(vals: dict[str, str | None], env_name: str, *file_keys: str) -> None:
    if (os.getenv(env_name) or "").strip():
        return
    for k in file_keys:
        raw = vals.get(k)
        if raw is not None and str(raw).strip():
            os.environ[env_name] = str(raw).strip()
            return


def load_registry_environment(repo_root: Path) -> None:
    """
    Load ``.env`` then fill **only blank** process variables from the file.

    ``load_dotenv(override=False)`` skips keys already set in the environment—even to an
    empty string—so IDEs/shells often block real values. This merges ``dotenv_values``
    for critical keys when the live env is missing or whitespace-only.
    """
    env_path = repo_root / ".env"
    load_dotenv(env_path, override=False)
    if not env_path.is_file():
        return
    vals = dotenv_values(env_path)
    _set_if_blank_from_file(vals, "POSTGRES_DSN", "POSTGRES_DSN")
    _set_if_blank_from_file(vals, "MONGODB_URI", "MONGODB_URI", "MONGO_URI", "MONGODB_URL")
    _set_if_blank_from_file(vals, "MONGODB_DATABASE", "MONGODB_DATABASE")
    _set_if_blank_from_file(vals, "SQLITE_PATH", "SQLITE_PATH")
    _set_if_blank_from_file(vals, "DUCKDB_PATH", "DUCKDB_PATH")
    _set_if_blank_from_file(vals, "ORACLE_FORGE_REGISTRY_POSTGRES_DSN", "ORACLE_FORGE_REGISTRY_POSTGRES_DSN")
    _set_if_blank_from_file(vals, "ORACLE_FORGE_REGISTRY_MONGODB_URI", "ORACLE_FORGE_REGISTRY_MONGODB_URI")
    for env_name, file_key in (
        ("POSTGRES_HOST", "POSTGRES_HOST"),
        ("PGHOST", "PGHOST"),
        ("POSTGRES_PORT", "POSTGRES_PORT"),
        ("PGPORT", "PGPORT"),
        ("POSTGRES_USER", "POSTGRES_USER"),
        ("PGUSER", "PGUSER"),
        ("POSTGRES_PASSWORD", "POSTGRES_PASSWORD"),
        ("PGPASSWORD", "PGPASSWORD"),
        ("POSTGRES_DB", "POSTGRES_DB"),
        ("PGDATABASE", "PGDATABASE"),
        ("DATABASE_URL", "DATABASE_URL"),
        ("MONGO_HOST", "MONGO_HOST"),
        ("MONGODB_HOST", "MONGODB_HOST"),
        ("MONGO_PORT", "MONGO_PORT"),
        ("MONGODB_PORT", "MONGODB_PORT"),
        ("MONGO_USER", "MONGO_USER"),
        ("MONGO_PASSWORD", "MONGO_PASSWORD"),
    ):
        _set_if_blank_from_file(vals, env_name, file_key)


def resolved_postgres_dsn() -> str:
    """
    Precedence:
    1. ORACLE_FORGE_REGISTRY_POSTGRES_DSN
    2. POSTGRES_DSN
    3. DATABASE_URL (if postgres)
    4. Built from POSTGRES_HOST / PGHOST + port, user, password, db (docker-compose style)
    """
    r = (os.getenv("ORACLE_FORGE_REGISTRY_POSTGRES_DSN") or "").strip()
    if r:
        return r
    r = (os.getenv("POSTGRES_DSN") or "").strip()
    if r:
        return r
    r = (os.getenv("DATABASE_URL") or "").strip()
    if r.lower().startswith("postgres"):
        return r
    host = (os.getenv("POSTGRES_HOST") or os.getenv("PGHOST") or "").strip()
    if not host:
        return ""
    port = (os.getenv("POSTGRES_PORT") or os.getenv("PGPORT") or "5432").strip()
    user = (os.getenv("POSTGRES_USER") or os.getenv("PGUSER") or "postgres").strip()
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
    db = (os.getenv("POSTGRES_DB") or os.getenv("PGDATABASE") or "postgres").strip()
    if password:
        auth = f"{quote_plus(user)}:{quote_plus(password)}"
    else:
        auth = quote_plus(user)
    return f"postgresql://{auth}@{host}:{port}/{db}"


def resolved_mongodb_uri() -> str:
    """
    Precedence:
    1. ORACLE_FORGE_REGISTRY_MONGODB_URI
    2. MONGODB_URI / MONGO_URI (filled by load_registry_environment)
    3. Built from MONGO_HOST + port + optional credentials
    """
    r = (os.getenv("ORACLE_FORGE_REGISTRY_MONGODB_URI") or "").strip()
    if r:
        return r
    r = (os.getenv("MONGODB_URI") or "").strip()
    if r:
        return r
    host = (os.getenv("MONGO_HOST") or os.getenv("MONGODB_HOST") or "").strip()
    if not host:
        return ""
    port = (os.getenv("MONGO_PORT") or os.getenv("MONGODB_PORT") or "27017").strip()
    user = (os.getenv("MONGO_USER") or "").strip()
    password = (os.getenv("MONGO_PASSWORD") or "").strip()
    if user and password:
        return f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/"
    return f"mongodb://{host}:{port}/"


def mongodb_database_name(dataset_mongo_from_profile: str | None) -> str:
    """Prefer per-dataset profile name, then MONGODB_DATABASE env."""
    if (dataset_mongo_from_profile or "").strip():
        return str(dataset_mongo_from_profile).strip()
    return (os.getenv("MONGODB_DATABASE") or "").strip()
