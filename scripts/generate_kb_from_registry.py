"""CLI: generate `kb/generated/authoritative/<dataset>.md` from `artifacts/schema_registry/<dataset>.json`."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from utils.schema_registry.kb_generator import write_authoritative_kb


def _dataset_ids_from_config(repo_root: Path) -> list[str]:
    cfg = os.getenv("ORACLE_FORGE_DATASETS_CONFIG", "").strip()
    path = Path(cfg) if cfg else repo_root / "eval" / "datasets.json"
    if not path.is_file():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    block = data.get("datasets") or {}
    if not isinstance(block, dict):
        return []
    return sorted(k for k in block if k != "comment" and isinstance(block.get(k), dict))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate AUTHORITATIVE KB markdown from schema registry JSON.",
    )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--dataset", help="Dataset id (e.g. yelp)")
    grp.add_argument(
        "--all-datasets",
        action="store_true",
        help="Generate KB for every entry in eval/datasets.json",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Do not append to logs/kb_generation.jsonl",
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]

    log_kb = not args.no_log
    if args.all_datasets:
        ids = _dataset_ids_from_config(repo_root)
        if not ids:
            print("No datasets in eval/datasets.json", file=sys.stderr)
            return 1
        rc = 0
        for did in ids:
            try:
                path, _ = write_authoritative_kb(did, repo_root, log=log_kb)
                print(path)
            except Exception as exc:
                print(f"{did}: {exc}", file=sys.stderr)
                rc = 2
        return rc

    assert args.dataset is not None
    try:
        path, _ = write_authoritative_kb(args.dataset.strip(), repo_root, log=log_kb)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
