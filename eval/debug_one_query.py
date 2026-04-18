"""
Run one DataAgentBench query with full agent output + evaluator validation + ground truth.

Usage (from repo root, with .venv activated):

  python eval/debug_one_query.py --dataset yelp --index 0

  python eval/debug_one_query.py --dataset DEPS_DEV_V1 --index 0 --compact

  python eval/debug_one_query.py --dataset yelp --index 2 --first-tool-trace

  python eval/debug_one_query.py --dataset yelp --index 0 --pipeline-debug

Environment: same as eval/run_dab_eval.py (load_dotenv .env).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.main import run_agent
from eval.evaluator import OracleForgeEvaluator
from utils.pipeline_debug_snapshot import extract_pipeline_debug


def _ground_truth_lines(query_case: Dict[str, Any]) -> Tuple[Optional[str], List[str]]:
    vpath = query_case.get("validator_path")
    if not vpath:
        return None, []
    parent = Path(vpath).parent
    gt = parent / "ground_truth.csv"
    if not gt.exists():
        return str(gt), []
    raw = [line.strip() for line in gt.read_text(encoding="utf-8").splitlines() if line.strip()]
    return str(gt), raw


def _first_tool_trace_entry(trace: Any) -> List[Dict[str, Any]]:
    """Return a one-element list with the first tool invocation record, or []."""
    if not isinstance(trace, list):
        return []
    for item in trace:
        if isinstance(item, dict) and item.get("tool_used"):
            return [item]
    return []


def main() -> None:
    load_dotenv(ROOT / ".env", override=True)
    parser = argparse.ArgumentParser(
        description="Debug one DAB query: agent response, validation, expected (ground truth)."
    )
    parser.add_argument(
        "--dataset",
        default=os.getenv("DAB_DATASET", "yelp"),
        help="Short DataAgentBench folder key (e.g. yelp, DEPS_DEV_V1). Default: DAB_DATASET or yelp.",
    )
    parser.add_argument(
        "--index",
        type=int,
        default=0,
        metavar="N",
        help="Zero-based index into that dataset's sorted query* folders (default: 0).",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Omit query_trace/plan from printed JSON (smaller output).",
    )
    parser.add_argument(
        "--first-tool-trace",
        action="store_true",
        help="Keep only the first tool call entry in trace/query_trace (drops retries and closed_loop events).",
    )
    parser.add_argument(
        "--pipeline-debug",
        action="store_true",
        help="Include Phase 9 pipeline_debug block (schema snapshot, routing proxy, validation, repairs, execution).",
    )
    args = parser.parse_args()

    evaluator = OracleForgeEvaluator(repo_root=ROOT)
    queries = evaluator.load_dataagentbench_queries(dataset=args.dataset.strip())
    if not queries:
        print(
            json.dumps(
                {
                    "error": "No queries loaded",
                    "hint": f"Check DataAgentBench/query_{args.dataset} exists and contains query*/query.json",
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    if args.index < 0 or args.index >= len(queries):
        print(
            json.dumps(
                {
                    "error": "index out of range",
                    "index": args.index,
                    "available": len(queries),
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    q: Dict[str, Any] = dict(queries[args.index])
    q["dataset"] = args.dataset.strip()

    outcome = run_agent(
        question=q["question"],
        available_databases=q["available_databases"],
        schema_info=q.get("schema_info", {}),
        dataset_id=q.get("dataset"),
    )
    valid, message = evaluator._validate_answer(q, outcome)

    gt_path, gt_lines = _ground_truth_lines(q)
    normalized_actual: Optional[List[str]] = None
    normalized_expected: Optional[List[str]] = None
    if gt_lines:
        normalized_actual = evaluator._normalize_execution_output(outcome.get("answer"))
        normalized_expected = evaluator._normalize_ground_truth(gt_lines)

    agent_block: Dict[str, Any] = dict(outcome)
    if args.compact:
        agent_block.pop("query_trace", None)
        agent_block.pop("trace", None)
        agent_block.pop("plan", None)
    elif args.first_tool_trace:
        agent_block["query_trace"] = _first_tool_trace_entry(agent_block.get("query_trace"))
        agent_block["trace"] = _first_tool_trace_entry(agent_block.get("trace"))

    report: Dict[str, Any] = {
        "dataset": q["dataset"],
        "query_id": q.get("id"),
        "index": args.index,
        "question": q["question"],
        "available_databases": q.get("available_databases"),
        "validator_path": q.get("validator_path"),
        "ground_truth_path": gt_path,
        "ground_truth_lines": gt_lines,
        "validation_ok": valid,
        "validation_message": message,
        "normalized_match": (
            normalized_actual == normalized_expected
            if normalized_actual is not None and normalized_expected is not None
            else None
        ),
        "normalized_actual": normalized_actual,
        "normalized_expected": normalized_expected,
        "llm_used_for_reasoning": outcome.get("architecture_disclosure", {}).get("llm_used_for_reasoning"),
        "agent": agent_block,
    }
    if args.pipeline_debug:
        report["pipeline_debug"] = extract_pipeline_debug(outcome, schema_info=q.get("schema_info", {}))

    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
