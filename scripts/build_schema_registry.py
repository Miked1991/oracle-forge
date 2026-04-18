"""CLI: build `artifacts/schema_registry/<dataset>.json` from live introspection."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from utils.schema_registry.builder import build_schema_registry


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
    parser = argparse.ArgumentParser(description="Build canonical schema registry JSON for a dataset.")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--dataset", help="Dataset id (e.g. yelp)")
    grp.add_argument(
        "--all-datasets",
        action="store_true",
        help="Build one registry file per entry in eval/datasets.json (keys under datasets).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for <dataset>.json (default: artifacts/schema_registry)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print registry JSON to stdout instead of writing a file (still introspects live DBs).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with non-zero status if no tables or collections were introspected.",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Do not append to logs/schema_registry.jsonl (ORACLE_FORGE_DISABLE_SCHEMA_REGISTRY_LOG also skips).",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[1]

    if args.all_datasets:
        ids = _dataset_ids_from_config(repo_root)
        if not ids:
            print("No datasets found in eval/datasets.json", file=sys.stderr)
            return 1
        rc = 0
        for did in ids:
            out: Path | None = None
            if args.output_dir:
                args.output_dir.mkdir(parents=True, exist_ok=True)
                safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in did.strip())
                out = args.output_dir / f"{safe}.json"
            try:
                _, written = build_schema_registry(
                    did,
                    repo_root=repo_root,
                    output_path=out,
                    log=not args.no_log and not args.dry_run,
                    strict=args.strict,
                    persist=not args.dry_run,
                )
                if not args.dry_run:
                    print(written)
            except RuntimeError as exc:
                print(f"{did}: {exc}", file=sys.stderr)
                rc = 2
        return rc

    assert args.dataset is not None
    out = None
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in args.dataset.strip())
        out = args.output_dir / f"{safe}.json"

    try:
        registry, written = build_schema_registry(
            args.dataset,
            repo_root=repo_root,
            output_path=out,
            log=not args.no_log and not args.dry_run,
            strict=args.strict,
            persist=not args.dry_run,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.dry_run:
        print(json.dumps(registry, ensure_ascii=False, indent=2))
    elif out is None:
        print(written)
    else:
        print(written)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
