from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .context_builder import ContextBuilder
from .planner import QueryPlanner
from .sandbox_client import SandboxClient
from .tools_client import MCPToolsClient
from .utils import (
    compute_metrics,
    confidence_score,
    infer_join_key,
    join_records,
    sanitize_error,
)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _merge_outputs(step_outputs: List[Dict[str, Any]], trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    successful_data = [entry.get("data", []) for entry in step_outputs if entry.get("ok")]
    normalized = [rows if isinstance(rows, list) else [] for rows in successful_data]
    if not normalized:
        return []
    merged = normalized[0]
    for right_rows in normalized[1:]:
        left_key = infer_join_key(merged)
        right_key = infer_join_key(right_rows)
        if not left_key or not right_key:
            continue
        joined = join_records(merged, right_rows, left_key, right_key)
        trace.append(
            {
                "merge_event": True,
                "left_key": left_key,
                "right_key": right_key,
                "rows_before": len(merged),
                "rows_after": len(joined),
            }
        )
        merged = joined if joined else merged
    return merged


def _answer_from_metrics(question: str, metrics: Dict[str, Any], records: List[Dict[str, Any]]) -> Any:
    text = question.lower()
    if "negative" in text and "sentiment" in text:
        return metrics["negative_sentiment_count"]
    if "high-value" in text and "ticket" in text:
        return metrics["high_value_with_tickets"]
    if "total sales" in text or "total revenue" in text:
        return metrics["total_sales"]
    if "how many" in text or "count" in text:
        return metrics["row_count"]
    return {"metrics": metrics, "records": records[:10]}


def _tool_payload(step: Dict[str, Any], question: str) -> Dict[str, Any]:
    payload = dict(step.get("query_payload", {}))
    payload["question"] = question
    payload["database"] = step.get("database")
    payload["dialect"] = step.get("dialect")
    return payload


def _record_runtime_corrections(question: str, plan: Dict[str, Any], tool_results: List[Dict[str, Any]]) -> None:
    failures = [item for item in tool_results if not item.get("ok")]
    if not failures:
        return
    repo_root = Path(__file__).resolve().parents[1]
    target = repo_root / "docs" / "driver_notes" / "runtime_corrections.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        for failure in failures:
            payload = {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "question": question,
                "failure_type": failure.get("error_type", "unknown_error"),
                "sanitized_error": sanitize_error(failure.get("error", "")),
                "tool": failure.get("tool"),
                "failed_query": failure.get("failed_query"),
                "plan_type": plan.get("plan_type"),
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def run_agent(question: str, available_databases: List[str], schema_info: Dict[str, Any]) -> Dict[str, Any]:
    trace: List[Dict[str, Any]] = []
    mock_mode = _env_bool("ORACLE_FORGE_MOCK_MODE", True)
    tools = MCPToolsClient(
        base_url=os.getenv("MCP_BASE_URL", "http://localhost:5000"),
        mock_mode=mock_mode,
    )
    discovered_tools = tools.discover_tools()
    discovered_schema = tools.get_schema_metadata()
    context = ContextBuilder().build(question, available_databases, schema_info, discovered_schema)
    plan = QueryPlanner(context).create_plan(question, available_databases)
    sandbox = SandboxClient(enabled=True)
    used_databases: List[Dict[str, str]] = []
    retries = 0

    def _execute(step: Dict[str, Any]) -> Dict[str, Any]:
        tool_name = tools.select_tool(step.get("database", ""), step.get("dialect", "sql"))
        if not tool_name:
            return {
                "ok": False,
                "error": f"No compatible tool discovered for database: {step.get('database')}",
                "tool": "",
                "failed_query": str(step.get("query_payload")),
            }
        used_databases.append(
            {
                "database": step.get("database", ""),
                "reason": step.get("selection_reason", ""),
                "tool": tool_name,
            }
        )
        result = tools.execute_with_retry(
            tool_name=tool_name,
            payload=_tool_payload(step, question),
            selection_reason=step.get("selection_reason", ""),
            dialect_handling=step.get("dialect", "sql"),
            trace=trace,
            max_retries=2,
        )
        return result

    sandbox_outcome = sandbox.execute_plan(plan, _execute)
    tool_results = sandbox_outcome["result"]
    _record_runtime_corrections(question, plan, tool_results)
    retries = sum(max(0, int(item.get("attempts", 1)) - 1) for item in tool_results)
    successful_steps = sum(1 for item in tool_results if item.get("ok"))
    predicted_queries = [
        {
            "database": step.get("database"),
            "dialect": step.get("dialect"),
            "query": step.get("query_payload", {}).get("sql", step.get("query_payload", {}).get("pipeline")),
        }
        for step in plan.get("steps", [])
    ]

    if successful_steps == 0:
        return {
            "status": "failure",
            "question": question,
            "answer": None,
            "confidence": confidence_score(
                total_steps=max(1, len(plan.get("steps", []))),
                successful_steps=0,
                retries=retries,
                explicit_failure=True,
                used_mock_mode=mock_mode,
            ),
            "trace": trace,
            "plan": plan,
            "used_databases": used_databases,
            "validation_status": sandbox_outcome["validation_status"],
            "error": "All tool executions failed; no verified result available.",
            "error_summary": [sanitize_error(item.get("error", "")) for item in tool_results if not item.get("ok")],
            "predicted_queries": predicted_queries,
        }

    merged_records = _merge_outputs(tool_results, trace)
    metrics = compute_metrics(merged_records)
    answer = _answer_from_metrics(question, metrics, merged_records)
    explicit_failure = not sandbox_outcome["validation_status"]["valid"]
    confidence = confidence_score(
        total_steps=max(1, len(plan.get("steps", []))),
        successful_steps=successful_steps,
        retries=retries,
        explicit_failure=explicit_failure,
        used_mock_mode=mock_mode,
    )
    return {
        "status": "success" if not explicit_failure else "partial_success",
        "question": question,
        "answer": answer,
        "metrics": metrics,
        "confidence": confidence,
        "trace": trace,
        "plan": plan,
        "tools_discovered_count": len(discovered_tools),
        "used_databases": used_databases,
        "validation_status": sandbox_outcome["validation_status"],
        "mock_mode": mock_mode,
        "predicted_queries": predicted_queries,
    }

def main() -> None:
    parser = argparse.ArgumentParser(description="Oracle Forge agent runner")
    parser.add_argument("--question", required=True, help="Natural language question")
    parser.add_argument(
        "--dbs",
        default="postgresql,mongodb,sqlite,duckdb",
        help="Comma-separated available database names",
    )
    args = parser.parse_args()
    databases = [item.strip() for item in args.dbs.split(",") if item.strip()]
    result = run_agent(args.question, databases, {})
    print(result)


if __name__ == "__main__":
    main()
