"""
Run the agent once and print a short report: generated query, response, expected (DAB), validation.

No large JSON dumps. Same environment as other eval scripts (``load_dotenv .env``).

Examples::

  python eval/query_agent_concise.py --dataset yelp --index 0

  python eval/query_agent_concise.py -q "What is the average rating?" --dataset-id yelp \\
      --dbs postgresql,mongodb,duckdb

  When ``--dataset-id`` is set (e.g. yelp) but ``--dataset`` is not, the script tries to
  find a DataAgentBench query with the **exact same question** and then binds ground truth
  / validation. Use ``--no-auto-dab`` to disable.

"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agent.main import run_agent
from eval.evaluator import OracleForgeEvaluator


def _find_dab_case_by_exact_question(
    evaluator: OracleForgeEvaluator,
    dataset_key: str,
    question: str,
) -> Optional[Dict[str, Any]]:
    """Return a query_case dict if ``question`` equals a loaded DAB question (strip match)."""
    queries = evaluator.load_dataagentbench_queries(dataset=dataset_key)
    want = question.strip()
    for row in queries:
        q = str(row.get("question", "")).strip()
        if q == want:
            return dict(row)
    return None


def _ground_truth_lines(query_case: Dict[str, Any]) -> Tuple[Optional[Path], List[str]]:
    vpath = query_case.get("validator_path")
    if not vpath:
        return None, []
    parent = Path(vpath).parent
    gt = parent / "ground_truth.csv"
    if not gt.is_file():
        return gt, []
    raw = [line.strip() for line in gt.read_text(encoding="utf-8").splitlines() if line.strip()]
    return gt, raw


def _format_predicted(outcome: Dict[str, Any]) -> str:
    preds = outcome.get("predicted_queries") or []
    if not preds:
        return "(none)"
    lines: List[str] = []
    for p in preds:
        if not isinstance(p, dict):
            continue
        db = p.get("database", "?")
        q = p.get("query") or p.get("sql") or ""
        lines.append(f"[{db}] {q}".strip())
    return "\n".join(lines) if lines else "(none)"


def _format_answer(answer: Any, max_chars: int = 1200) -> str:
    if answer is None:
        return "(null)"
    try:
        s = json.dumps(answer, ensure_ascii=False, indent=2)
    except TypeError:
        s = str(answer)
    if len(s) > max_chars:
        return s[: max_chars - 40] + "\n... (truncated)"
    return s


def _format_database_results(raw: Any, max_chars: int = 4000) -> str:
    """Pretty-print per-database row payloads returned from tool execution (not the shaped answer)."""
    if raw is None:
        return "(null)"
    if not raw:
        return "(none)"
    try:
        s = json.dumps(raw, ensure_ascii=False, indent=2)
    except TypeError:
        s = str(raw)
    if len(s) > max_chars:
        return s[: max_chars - 40] + "\n... (truncated)"
    return s


def main() -> None:
    load_dotenv(ROOT / ".env", override=True)
    parser = argparse.ArgumentParser(
        description="Concise agent test: generated query, answer, ground truth, validation."
    )
    parser.add_argument(
        "-q",
        "--question",
        default="",
        help="Natural-language question (required if --dataset is not set).",
    )
    parser.add_argument(
        "--dataset",
        default="",
        metavar="KEY",
        help="DataAgentBench folder key (e.g. yelp). Loads query from query_<KEY>/queryN.",
    )
    parser.add_argument(
        "--index",
        type=int,
        default=0,
        help="Zero-based query index when --dataset is set (default: 0).",
    )
    parser.add_argument(
        "--dataset-id",
        default="",
        help="Passed to run_agent as dataset_id (e.g. yelp). For ad-hoc --question, set explicitly.",
    )
    parser.add_argument(
        "--dbs",
        default="",
        help="Comma-separated databases (default: from DAB db_config or postgresql,mongodb,sqlite,duckdb).",
    )
    parser.add_argument(
        "--no-auto-dab",
        action="store_true",
        help="Do not match --question to a DAB case when --dataset-id is set.",
    )
    args = parser.parse_args()

    evaluator = OracleForgeEvaluator(repo_root=ROOT)
    question = (args.question or "").strip()
    dataset_id: Optional[str] = None
    schema_info: Dict[str, Any] = {}
    available: List[str] = []
    dab_auto_note = ""

    query_case: Dict[str, Any] = {}
    if (args.dataset or "").strip():
        ds = args.dataset.strip()
        queries = evaluator.load_dataagentbench_queries(dataset=ds)
        if not queries:
            print(f"No queries loaded for dataset {ds!r}.", file=sys.stderr)
            sys.exit(1)
        if args.index < 0 or args.index >= len(queries):
            print(f"index {args.index} out of range (0..{len(queries) - 1}).", file=sys.stderr)
            sys.exit(1)
        query_case = dict(queries[args.index])
        query_case["dataset"] = ds
        question = str(query_case["question"])
        dataset_id = ds
        schema_info = query_case.get("schema_info") or {}
        available = list(query_case.get("available_databases") or [])
    else:
        if not question:
            print("Provide --question or --dataset.", file=sys.stderr)
            sys.exit(2)
        dataset_id = (args.dataset_id or "").strip() or None
        if (
            dataset_id
            and not args.no_auto_dab
            and not query_case.get("validator_path")
        ):
            matched = _find_dab_case_by_exact_question(evaluator, dataset_id, question)
            if matched:
                query_case = matched
                query_case["dataset"] = dataset_id
                schema_info = query_case.get("schema_info") or {}
                if not (args.dbs or "").strip():
                    available = list(query_case.get("available_databases") or [])
                vp = query_case.get("validator_path")
                qdir = Path(str(vp)).parent.name if vp else "?"
                dab_auto_note = f"(auto-matched DAB case: {qdir})"

    if (args.dbs or "").strip():
        available = [x.strip().lower() for x in args.dbs.split(",") if x.strip()]
    if not available:
        available = ["postgresql", "mongodb", "sqlite", "duckdb"]

    outcome = run_agent(
        question=question,
        available_databases=available,
        schema_info=schema_info,
        dataset_id=dataset_id,
    )

    valid: bool = False
    message = (
        "n/a (use --dataset KEY --index N, or the same wording as a DAB query with --dataset-id KEY)"
    )
    gt_path: Optional[Path] = None
    gt_lines: List[str] = []
    normalized_actual: Optional[List[str]] = None
    normalized_expected: Optional[List[str]] = None

    if query_case.get("validator_path"):
        valid, message = evaluator._validate_answer(query_case, outcome)
        gt_path, gt_lines = _ground_truth_lines(query_case)
        if gt_lines:
            normalized_actual = evaluator._normalize_execution_output(outcome.get("answer"))
            normalized_expected = evaluator._normalize_ground_truth(gt_lines)
    elif query_case.get("expected"):
        valid, message = evaluator._validate_expected(query_case["expected"], outcome.get("answer"))

    # --- print ---
    print("=== Question ===")
    print(question)
    if dab_auto_note:
        print(dab_auto_note)
    print()
    print("=== Generated query (predicted) ===")
    print(_format_predicted(outcome))
    print()
    print("=== Agent response (answer) ===")
    print(_format_answer(outcome.get("answer")))
    print()
    print("=== Database response (raw rows from tools) ===")
    print(_format_database_results(outcome.get("database_results")))
    print()
    print("=== Expected (benchmark) ===")
    if gt_path is not None:
        print(f"ground_truth: {gt_path}")
        for line in gt_lines[:20]:
            print(f"  {line}")
        if len(gt_lines) > 20:
            print(f"  ... ({len(gt_lines)} lines total)")
    else:
        print(
            "(no ground_truth.csv — use ``--dataset yelp --index 0`` or match a DAB question with "
            "``-q`` + ``--dataset-id yelp``)"
        )
    print()
    print("=== Validation ===")
    print(f"ok: {valid}")
    print(f"message: {message}")
    if normalized_actual is not None and normalized_expected is not None:
        print(f"normalized_actual:   {normalized_actual}")
        print(f"normalized_expected: {normalized_expected}")
        print(f"match: {normalized_actual == normalized_expected}")
    print()
    print("=== Run summary ===")
    print(f"status: {outcome.get('status')} | confidence: {outcome.get('confidence')}")
    if outcome.get("predicted_queries"):
        tr = outcome.get("query_trace") or outcome.get("trace") or []
        for item in tr:
            if isinstance(item, dict) and item.get("tool_used"):
                print(f"tool: {item.get('tool_used')} | success: {item.get('success')}")
                break


if __name__ == "__main__":
    main()
