from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.main import run_agent
from eval.evaluator import OracleForgeEvaluator
from utils.pipeline_debug_snapshot import extract_pipeline_debug


def _parse_per_dataset_arg(raw: Optional[str]) -> Optional[int]:
    """None means all queries per dataset; int means cap."""
    if raw is None:
        return None
    text = str(raw).strip().lower()
    if text in {"", "all", "none"}:
        return None
    return int(text)


def _resolve_queries(
    evaluator: OracleForgeEvaluator,
    *,
    scope: str,
    dataset: str,
    per_dataset: Optional[int],
    datasets_csv: Optional[str],
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build query list and metadata for the results JSON."""
    meta: Dict[str, Any] = {"dab_scope": scope}
    if scope == "single":
        meta["dataset"] = dataset
        meta["per_dataset"] = None
        meta["datasets_included"] = [dataset]
        queries = evaluator.load_dataagentbench_queries(dataset=dataset)
        return queries, meta

    meta["dab_scope"] = "multi"
    keys: Optional[List[str]] = None
    if datasets_csv and datasets_csv.strip():
        keys = [x.strip() for x in datasets_csv.split(",") if x.strip()]
    meta["datasets_included"] = keys or evaluator.list_dataagentbench_dataset_keys()
    meta["per_dataset"] = per_dataset
    queries = evaluator.load_dataagentbench_queries_multi(
        per_dataset=per_dataset,
        datasets=keys,
    )
    return queries, meta


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "DataAgentBench evaluation: multi-trial pass@1. "
            "Use --scope multi to run the first N queries (or all) from each query_* dataset."
        )
    )
    parser.add_argument(
        "--scope",
        choices=["single", "multi"],
        default=os.getenv("DAB_SCOPE", "single"),
        help="single = one DataAgentBench folder; multi = many folders (default: env DAB_SCOPE or single).",
    )
    parser.add_argument(
        "--dataset",
        default=os.getenv("DAB_DATASET", "yelp"),
        help="When scope=single: short dataset key (default: env DAB_DATASET or yelp).",
    )
    parser.add_argument(
        "--per-dataset",
        default=os.getenv("DAB_QUERIES_PER_DATASET", "all"),
        metavar="N_OR_ALL",
        help="When scope=multi: first N queries per dataset, or 'all' (default: env DAB_QUERIES_PER_DATASET or all).",
    )
    parser.add_argument(
        "--datasets",
        default=os.getenv("DAB_DATASETS"),
        metavar="LIST",
        help="When scope=multi: comma-separated dataset keys (default: all query_* under DataAgentBench).",
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=int(os.getenv("DAB_TRIALS_PER_QUERY", "50")),
        help="Trials per query (default: env DAB_TRIALS_PER_QUERY or 50).",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=None,
        metavar="N",
        help="After loading queries, run only the first N (default: all). Useful for quick smoke evals.",
    )
    parser.add_argument(
        "--pipeline-debug",
        action="store_true",
        help="Phase 9: on trial 1 only, attach pipeline_debug to that trial (larger results.json).",
    )
    parser.add_argument(
        "--no-duckdb-enrich",
        action="store_true",
        help=(
            "Skip live DuckDB column introspection during schema enrich (sets "
            "ORACLE_FORGE_DUCKDB_SCHEMA_ENRICH=false). Use when eval hangs in duckdb.connect / DESCRIBE."
        ),
    )
    return parser


def main() -> None:
    load_dotenv(ROOT / ".env", override=True)
    args = build_parser().parse_args()
    if getattr(args, "no_duckdb_enrich", False):
        os.environ["ORACLE_FORGE_DUCKDB_SCHEMA_ENRICH"] = "false"
    eval_root = ROOT / "eval"
    eval_root.mkdir(parents=True, exist_ok=True)
    trials = max(1, args.trials)

    per_dataset: Optional[int] = _parse_per_dataset_arg(args.per_dataset)

    evaluator = OracleForgeEvaluator(repo_root=ROOT)
    queries, run_meta = _resolve_queries(
        evaluator,
        scope=args.scope,
        dataset=args.dataset.strip(),
        per_dataset=per_dataset,
        datasets_csv=args.datasets,
    )
    if args.max_queries is not None and args.max_queries > 0:
        queries = queries[: args.max_queries]
        run_meta["max_queries"] = args.max_queries

    all_query_reports: List[Dict[str, Any]] = []
    total_first_correct = 0
    total_trial_correct = 0
    total_trials = 0

    for query in queries:
        ds = query.get("dataset", run_meta.get("dataset"))
        composite_id = f"{ds}:{query['id']}" if ds else query["id"]

        trials_report: List[Dict[str, Any]] = []
        first_correct = False
        for trial in range(trials):
            result = run_agent(
                question=query["question"],
                available_databases=query["available_databases"],
                schema_info=query["schema_info"],
                dataset_id=query.get("dataset"),
            )
            valid, message = evaluator._validate_answer(query, result)
            if trial == 0:
                first_correct = bool(valid)
            if valid:
                total_trial_correct += 1
            total_trials += 1
            trial_row: Dict[str, Any] = {
                "trial": trial + 1,
                "correct": bool(valid),
                "eval_correct": bool(valid),
                "validation_message": message,
                "status": result.get("status"),
                "answer": result.get("answer"),
                "confidence": result.get("confidence"),
                "closed_loop": result.get("closed_loop"),
                "query_trace": result.get("query_trace", result.get("trace", [])),
                "token_usage": result.get("token_usage", {}),
                "used_databases": result.get("used_databases", []),
            }
            if args.pipeline_debug and trial == 0:
                trial_row["pipeline_debug"] = extract_pipeline_debug(
                    result, schema_info=query.get("schema_info", {})
                )
            trials_report.append(trial_row)

        if first_correct:
            total_first_correct += 1

        all_query_reports.append(
            {
                "id": query["id"],
                "composite_id": composite_id,
                "dataset": ds,
                "question": query["question"],
                "first_trial_correct": first_correct,
                "trial_accuracy": round(sum(1 for item in trials_report if item["correct"]) / max(1, trials), 4),
                "trials": trials_report,
            }
        )

    total_queries = len(queries)
    pass_at_1 = round(total_first_correct / max(1, total_queries), 4)
    overall_trial_accuracy = round(total_trial_correct / max(1, total_trials), 4)

    per_dataset_summary: Dict[str, Any] = {}
    for row in all_query_reports:
        ds_key = str(row.get("dataset") or run_meta.get("dataset") or "unknown")
        bucket = per_dataset_summary.setdefault(ds_key, {"queries": 0, "first_trial_correct": 0})
        bucket["queries"] += 1
        if row.get("first_trial_correct"):
            bucket["first_trial_correct"] += 1
    for ds_key, bucket in per_dataset_summary.items():
        n = max(1, int(bucket["queries"]))
        bucket["pass@1"] = round(int(bucket["first_trial_correct"]) / n, 4)

    if args.scope == "multi":
        dataset_label = f"multi ({run_meta.get('datasets_included')})"
        dataset_path_note = "DataAgentBench/query_* (see datasets_included)"
    else:
        d = args.dataset.strip()
        dataset_label = f"DataAgentBench {d}"
        dataset_path = ROOT / "DataAgentBench"
        if d.lower().startswith("query_"):
            dataset_path = dataset_path / d
        else:
            dataset_path = dataset_path / f"query_{d}"
        dataset_path_note = str(dataset_path)

    results = {
        "dataset": dataset_label,
        "dataset_path": dataset_path_note,
        "run": run_meta,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_queries": total_queries,
        "trials_per_query": trials,
        "correct_first_answers": total_first_correct,
        "correct_trials": total_trial_correct,
        "total_trials": total_trials,
        "pass@1": pass_at_1,
        "overall_trial_accuracy": overall_trial_accuracy,
        "per_dataset_summary": per_dataset_summary,
        "queries": all_query_reports,
    }

    results_path = eval_root / "results.json"
    score_log_path = eval_root / "score_log.jsonl"
    results_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")

    score_entry = {
        "stage": "final",
        "dataset": dataset_label,
        "dab_scope": run_meta.get("dab_scope"),
        "datasets_included": run_meta.get("datasets_included"),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_queries": total_queries,
        "trials_per_query": trials,
        "correct_first_answers": total_first_correct,
        "correct_trials": total_trial_correct,
        "total_trials": total_trials,
        "pass@1": pass_at_1,
        "overall_trial_accuracy": overall_trial_accuracy,
        "per_dataset_summary": per_dataset_summary,
    }
    with score_log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(score_entry, ensure_ascii=False) + "\n")

    print(
        json.dumps(
            {
                "dab_scope": args.scope,
                "datasets_included": run_meta.get("datasets_included"),
                "per_dataset_cap": run_meta.get("per_dataset"),
                "total_queries": total_queries,
                "trials_per_query": trials,
                "pass@1": pass_at_1,
                "pass@1_counts": f"{total_first_correct}/{total_queries} queries with trial 1 correct",
                "overall_trial_accuracy": overall_trial_accuracy,
                "overall_trial_counts": f"{total_trial_correct}/{total_trials} trials correct",
                "note": "pass@1 and overall_trial_accuracy can match numerically when only some queries pass all trials; they measure different things.",
                "results_path": str(results_path),
                "score_log_path": str(score_log_path),
                "per_dataset_summary": per_dataset_summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
