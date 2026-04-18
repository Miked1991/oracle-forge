from __future__ import annotations

import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from agent.main import run_agent
from agent.utils import wilson_interval


class OracleForgeEvaluator:
    def __init__(self, repo_root: Optional[Path] = None) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[1]
        self.logs_dir = self.repo_root / "eval"
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def evaluate_queries(self, queries: List[Dict[str, Any]]) -> Dict[str, Any]:
        per_query: List[Dict[str, Any]] = []
        passed = 0
        sentinel_log = self.logs_dir / "sentinel_trace.jsonl"
        for idx, query_case in enumerate(queries):
            question = query_case["question"]
            started = time.perf_counter()
            outcome = run_agent(
                question=question,
                available_databases=query_case.get("available_databases", ["postgresql", "mongodb"]),
                schema_info=query_case.get("schema_info", {}),
                dataset_id=query_case.get("dataset"),
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            validation = self._validate_answer(query_case, outcome)
            correct = validation[0]
            if correct:
                passed += 1
            trace = outcome.get("query_trace", outcome.get("trace", []))
            row: Dict[str, Any] = {
                "query_id": query_case.get("id", f"query_{idx}"),
                "dataset": query_case.get("dataset"),
                "input_query": question,
                "generated_query": outcome.get("predicted_queries", []),
                "tool_calls": trace,
                "closed_loop": outcome.get("closed_loop"),
                "execution_time_ms": duration_ms,
                "result": outcome.get("answer"),
                "correctness": "pass" if correct else "fail",
                "validation_message": validation[1],
                "failure_category": None if correct else self._classify_failure(query_case, outcome),
                "architecture_disclosure": outcome.get("architecture_disclosure", {}),
            }
            if os.getenv("ORACLE_FORGE_EVAL_PIPELINE_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}:
                from utils.pipeline_debug_snapshot import extract_pipeline_debug

                row["pipeline_debug"] = extract_pipeline_debug(
                    outcome, schema_info=query_case.get("schema_info", {})
                )
            per_query.append(row)
            sentinel_payload = {
                "query_id": query_case.get("id", f"query_{idx}"),
                "closed_loop": outcome.get("closed_loop"),
                "tool_calls": [
                    {
                        "tool_used": event.get("tool_used"),
                        "raw_query": event.get("raw_query"),
                        "execution_time_ms": event.get("duration_ms"),
                        "success": event.get("success"),
                        "failure_type": event.get("failure_type"),
                        "merge_event": event.get("merge_event", False),
                        "merge_step": {
                            "left_key": event.get("left_key"),
                            "right_key": event.get("right_key"),
                            "rows_before": event.get("rows_before"),
                            "rows_after": event.get("rows_after"),
                        }
                        if event.get("merge_event")
                        else None,
                    }
                    for event in trace
                ],
                "final_status": outcome.get("status"),
                "answer": outcome.get("answer"),
                "confidence": outcome.get("confidence"),
            }
            with sentinel_log.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sentinel_payload, ensure_ascii=False) + "\n")
        total = len(per_query)
        pass_at_1 = (passed / total) if total else 0.0
        ci_low, ci_high = wilson_interval(passed, total)
        report = {
            "total_queries": total,
            "correct_queries": passed,
            "pass@1": round(pass_at_1, 4),
            "confidence_interval_95": [round(ci_low, 4), round(ci_high, 4)],
            "per_query_traces": per_query,
        }
        return report

    def evaluate_yelp_dataset(self) -> Dict[str, Any]:
        queries = self.load_dataagentbench_queries(dataset="yelp")
        return self.evaluate_queries(queries)

    def load_dataagentbench_queries(self, dataset: str = "yelp") -> List[Dict[str, Any]]:
        dab_root = self.repo_root / "DataAgentBench"
        if not dab_root.exists():
            return []

        dataset_key = dataset.strip()
        if dataset_key.lower().startswith("query_"):
            dataset_dir_name = dataset_key
        else:
            dataset_dir_name = f"query_{dataset_key}"

        dataset_dir = dab_root / dataset_dir_name
        if not dataset_dir.exists():
            dataset_dir = next(
                (p for p in dab_root.iterdir() if p.is_dir() and p.name.lower() == dataset_dir_name.lower()),
                Path(""),
            )
        if not dataset_dir.exists():
            return []

        available_databases = self._extract_db_types(dataset_dir)
        queries: List[Dict[str, Any]] = []
        for query_dir in sorted(dataset_dir.glob("query*"), key=self._query_sort_key):
            if not query_dir.is_dir():
                continue
            query_path = query_dir / "query.json"
            if not query_path.exists():
                continue

            query_payload = json.loads(query_path.read_text(encoding="utf-8"))
            if isinstance(query_payload, str):
                question = query_payload
            elif isinstance(query_payload, dict) and "query" in query_payload:
                question = str(query_payload["query"])
            else:
                continue

            queries.append(
                {
                    "id": query_dir.name,
                    "question": question,
                    "available_databases": available_databases,
                    "schema_info": {},
                    "validator_path": str(query_dir / "validate.py"),
                }
            )
        return queries

    def _load_dataagentbench_yelp_queries(self) -> List[Dict[str, Any]]:
        return self.load_dataagentbench_queries(dataset="yelp")

    def list_dataagentbench_dataset_keys(self) -> List[str]:
        """Short names for each `DataAgentBench/query_*` directory (e.g. `yelp`, `DEPS_DEV_V1`)."""
        dab_root = self.repo_root / "DataAgentBench"
        if not dab_root.exists():
            return []
        keys: List[str] = []
        for path in sorted(dab_root.iterdir()):
            if path.is_dir() and path.name.startswith("query_"):
                keys.append(path.name[len("query_") :])
        return keys

    def load_dataagentbench_queries_multi(
        self,
        *,
        per_dataset: Optional[int] = None,
        datasets: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Load queries from multiple DAB folders. Each item includes `dataset` (short key).

        `per_dataset`: None = all queries in each folder; otherwise first N per folder (sorted).
        `datasets`: None = all discovered `query_*` datasets; otherwise a subset of short keys.
        """
        keys = [k.strip() for k in (datasets or self.list_dataagentbench_dataset_keys()) if k.strip()]
        combined: List[Dict[str, Any]] = []
        for key in keys:
            rows = self.load_dataagentbench_queries(dataset=key)
            if per_dataset is not None:
                rows = rows[: max(0, per_dataset)]
            for row in rows:
                item = dict(row)
                item["dataset"] = key
                combined.append(item)
        return combined

    def _extract_db_types(self, dataset_dir: Path) -> List[str]:
        db_config_path = dataset_dir / "db_config.yaml"
        if not db_config_path.exists():
            return ["postgresql", "mongodb", "sqlite", "duckdb"]

        db_type_map = {
            "mongo": "mongodb",
            "mongodb": "mongodb",
            "postgres": "postgresql",
            "postgresql": "postgresql",
            "sqlite": "sqlite",
            "duckdb": "duckdb",
        }
        found: List[str] = []
        for raw_line in db_config_path.read_text(encoding="utf-8").splitlines():
            match = re.search(r"^\s*db_type:\s*([A-Za-z0-9_]+)", raw_line)
            if not match:
                continue
            db_key = match.group(1).strip().lower()
            normalized = db_type_map.get(db_key)
            if normalized and normalized not in found:
                found.append(normalized)

        return found or ["postgresql", "mongodb", "sqlite", "duckdb"]

    @staticmethod
    def _query_sort_key(path: Path) -> Tuple[int, str]:
        name = path.name
        suffix = name[5:] if name.startswith("query") else ""
        if suffix.isdigit():
            return (int(suffix), name)
        return (10**9, name)

    def _validate_answer(self, query_case: Dict[str, Any], outcome: Dict[str, Any]) -> Tuple[bool, str]:
        validator_path = query_case.get("validator_path")
        if not validator_path:
            expected = query_case.get("expected", {})
            return self._validate_expected(expected, outcome.get("answer"))
        path = Path(validator_path)
        if not path.exists():
            return False, f"Validator missing: {validator_path}"
        ground_truth_path = path.parent / "ground_truth.csv"
        if ground_truth_path.exists():
            return self._validate_against_ground_truth(ground_truth_path, outcome.get("answer"))
        namespace: Dict[str, Any] = {}
        exec(path.read_text(encoding="utf-8"), namespace)
        validate_fn = namespace.get("validate")
        if not callable(validate_fn):
            return False, "Validator function not found."
        answer_text = self._stringify_answer(outcome.get("answer"))
        valid, message = validate_fn(answer_text)
        return bool(valid), str(message)

    def _validate_against_ground_truth(self, ground_truth_path: Path, actual: Any) -> Tuple[bool, str]:
        expected_raw = [line.strip() for line in ground_truth_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not expected_raw:
            return False, "Ground truth file empty."
        actual_norm = self._normalize_execution_output(actual)
        expected_norm = self._normalize_ground_truth(expected_raw)
        if actual_norm == expected_norm:
            return True, "execution_match=True"
        if self._multiset_equal_fuzzy(actual_norm, expected_norm):
            return True, "execution_match=True(fuzzy_numeric)"
        return False, "execution_match=False"

    @staticmethod
    def _multiset_equal_fuzzy(actual: List[str], expected: List[str]) -> bool:
        """Order-independent multiset equality with approximate float matching."""
        if len(actual) != len(expected):
            return False
        exp = list(expected)
        for a in actual:
            idx = OracleForgeEvaluator._find_fuzzy_match_index(a, exp)
            if idx is None:
                return False
            exp.pop(idx)
        return True

    @staticmethod
    def _find_fuzzy_match_index(token: str, candidates: List[str]) -> Optional[int]:
        for i, e in enumerate(candidates):
            if OracleForgeEvaluator._scalar_equal_relaxed(token, e):
                return i
        return None

    @staticmethod
    def _scalar_equal_relaxed(a: str, b: str) -> bool:
        if a == b:
            return True
        try:
            fa, fb = float(a), float(b)
            if math.isclose(fa, fb, rel_tol=1e-9, abs_tol=1e-5):
                return True
        except (TypeError, ValueError):
            pass
        return a.lower() == b.lower()

    def _normalize_execution_output(self, actual: Any) -> List[str]:
        if isinstance(actual, list):
            values = [self._norm_scalar(item) for item in actual]
            return sorted(values)
        if isinstance(actual, dict):
            # Prefer row-shaped answers (tool results / merged query output) over raw metrics blobs.
            records = actual.get("records")
            if isinstance(records, list) and records:
                values: List[str] = []
                for row in records:
                    if isinstance(row, dict):
                        for cell in row.values():
                            values.append(self._norm_scalar(cell))
                    else:
                        values.append(self._norm_scalar(row))
                if values:
                    return sorted(values)
            flat_scalars: List[str] = []
            for value in actual.values():
                if isinstance(value, (dict, list)):
                    continue
                flat_scalars.append(self._norm_scalar(value))
            if flat_scalars:
                return sorted(flat_scalars)
            text = self._stringify_answer(actual)
            return sorted([self._norm_scalar(item) for item in text.split(",") if item.strip()])
        text = self._stringify_answer(actual)
        return sorted([self._norm_scalar(item) for item in text.split(",") if item.strip()])

    def _normalize_ground_truth(self, rows: List[str]) -> List[str]:
        values: List[str] = []
        for row in rows:
            for token in row.split(","):
                cleaned = token.strip()
                if cleaned:
                    values.append(self._norm_scalar(cleaned))
        return sorted(values)

    @staticmethod
    def _norm_scalar(value: Any) -> str:
        text = str(value).strip()
        try:
            return f"{float(text):.2f}"
        except Exception:
            return text.lower()

    def _validate_expected(self, expected: Dict[str, Any], actual: Any) -> Tuple[bool, str]:
        expected_type = expected.get("type", "equals")
        expected_value = expected.get("value")
        if expected_type == "equals":
            return actual == expected_value, f"equals({expected_value})"
        if expected_type == "contains":
            ok = str(expected_value).lower() in str(actual).lower()
            return ok, f"contains({expected_value})"
        if expected_type == "min":
            try:
                ok = float(actual) >= float(expected_value)
                return ok, f"min({expected_value})"
            except (TypeError, ValueError):
                return False, "min-compare-failed"
        return False, "unsupported-expected-type"

    def _classify_failure(self, query_case: Dict[str, Any], outcome: Dict[str, Any]) -> str:
        used_dbs = {item.get("database") for item in outcome.get("used_databases", [])}
        question = query_case.get("question", "").lower()
        trace = outcome.get("trace", [])
        if len(used_dbs) < 2 and any(word in question for word in ["across", "join", "both"]):
            return "multi-database routing errors"
        if any(entry.get("failure_type") == "join_key_mismatch" for entry in trace):
            return "join/key mismatch errors"
        return "domain knowledge gaps"

    @staticmethod
    def _stringify_answer(answer: Any) -> str:
        if isinstance(answer, (dict, list)):
            return json.dumps(answer, ensure_ascii=False)
        return str(answer)


class LocalEvaluator:
    def __init__(self, queries_path: Path) -> None:
        self.queries_path = queries_path
        self.queries = self._load_queries(queries_path)
        self.evaluator = OracleForgeEvaluator()

    def _load_queries(self, path: Path) -> List[Dict[str, Any]]:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload.get("queries", [])
        if isinstance(payload, list):
            return payload
        return []

    def evaluate(self) -> Dict[str, Any]:
        report = self.evaluator.evaluate_queries(self.queries)
        return {
            "total_queries": report["total_queries"],
            "correct_first_answers": report["correct_queries"],
            "pass@1": report["pass@1"],
            "confidence_interval_95": report["confidence_interval_95"],
            "results": report["per_query_traces"],
        }
