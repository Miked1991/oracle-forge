"""Print Postgres table row estimates and MongoDB database/collection counts (uses repo .env)."""

from __future__ import annotations

import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))

from dotenv import load_dotenv

load_dotenv(root / ".env", override=False)
from utils.schema_registry.env import load_registry_environment, resolved_mongodb_uri, resolved_postgres_dsn


def main() -> None:
    load_registry_environment(root)
    dsn = resolved_postgres_dsn()
    uri = resolved_mongodb_uri()
    print("POSTGRES_DSN:", "set" if dsn else "missing")
    print("MONGODB_URI:", "set" if uri else "missing")
    if dsn:
        try:
            import psycopg

            with psycopg.connect(dsn, connect_timeout=10) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT relname, COALESCE(n_live_tup, 0)::bigint
                        FROM pg_stat_user_tables
                        ORDER BY n_live_tup DESC NULLS LAST
                        LIMIT 25
                        """
                    )
                    print("\nPostgreSQL (pg_stat_user_tables, top 25):")
                    for name, n in cur.fetchall():
                        print(f"  {name}: {n}")
        except Exception as exc:
            print("PostgreSQL error:", exc)
    if uri:
        try:
            from pymongo import MongoClient

            client = MongoClient(uri, serverSelectionTimeoutMS=8000)
            client.admin.command("ping")
            print("\nMongoDB databases:", [d for d in client.list_database_names() if d not in ("admin", "config", "local")])
            for dbname in sorted(client.list_database_names()):
                if dbname in {"admin", "config", "local"}:
                    continue
                db = client[dbname]
                cols = [x for x in db.list_collection_names() if not x.startswith("system.")]
                sample = []
                for cname in cols[:12]:
                    try:
                        sample.append(f"{cname}≈{db[cname].estimated_document_count()}")
                    except Exception:
                        sample.append(cname)
                print(f"  {dbname}: {len(cols)} collections — {', '.join(sample)}")
            client.close()
        except Exception as exc:
            print("MongoDB error:", exc)


if __name__ == "__main__":
    main()
