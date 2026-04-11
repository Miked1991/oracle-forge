from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import httpx

from .utils import canonical_db_name, classify_failure, result_summary, sanitize_error


class MCPToolsClient:
    def __init__(
        self,
        base_url: str = "http://localhost:5000",
        mock_mode: bool = False,
        timeout_seconds: int = 12,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.mock_mode = mock_mode
        self.timeout_seconds = timeout_seconds
        self.discovered_tools: List[Dict[str, Any]] = []
        self.server_reachable = False
        self.client = httpx.Client(timeout=self.timeout_seconds)

    def discover_tools(self) -> List[Dict[str, Any]]:
        if self.mock_mode:
            self.discovered_tools = self._mock_tools_catalog()
            return self.discovered_tools
        endpoint = f"{self.base_url}/v1/tools"
        try:
            response = self.client.get(endpoint)
            response.raise_for_status()
            payload = response.json()
            tools = payload.get("tools", payload if isinstance(payload, list) else [])
            if not isinstance(tools, list):
                tools = []
            self.discovered_tools = [tool for tool in tools if isinstance(tool, dict)]
            self.server_reachable = True
            return self.discovered_tools
        except Exception:
            self.server_reachable = False
            self.mock_mode = True
            self.discovered_tools = self._mock_tools_catalog()
            return self.discovered_tools

    def get_schema_metadata(self) -> Dict[str, Any]:
        if self.mock_mode:
            return self._mock_schema_metadata()
        if not self.discovered_tools:
            self.discover_tools()
        metadata: Dict[str, Any] = {}
        schema_tools = [
            tool
            for tool in self.discovered_tools
            if any(
                token in f"{tool.get('name', '')} {tool.get('description', '')}".lower()
                for token in ["schema", "describe", "introspect", "collection", "table", "metadata"]
            )
        ]
        for tool in schema_tools:
            result = self._invoke_live(tool.get("name", ""), {"operation": "schema_discovery"})
            if not result.get("ok"):
                continue
            data = result.get("data")
            parsed = self._parse_schema_payload(data)
            for db_name, content in parsed.items():
                db = canonical_db_name(db_name)
                metadata.setdefault(db, {"tables": [], "collections": []})
                for key in ["tables", "collections"]:
                    for item in content.get(key, []):
                        if item not in metadata[db][key]:
                            metadata[db][key].append(item)
        return metadata or self._mock_schema_metadata()

    def select_tool(self, database: str, dialect: str) -> Optional[str]:
        if not self.discovered_tools:
            self.discover_tools()
        db = canonical_db_name(database)
        best_name = None
        best_score = float("-inf")
        for tool in self.discovered_tools:
            name = str(tool.get("name", ""))
            desc = str(tool.get("description", ""))
            combined = f"{name} {desc}".lower()
            score = 0
            if db in combined:
                score += 8
            if dialect == "mongodb_aggregation" and any(t in combined for t in ["mongo", "aggregation", "pipeline"]):
                score += 6
            if dialect == "sql" and any(t in combined for t in ["sql", "query", "postgres", "sqlite", "duckdb"]):
                score += 4
            if db != "mongodb" and "aggregation" in combined and "mongo" in combined:
                score -= 6
            if db == "mongodb" and "sql" in combined and "mongo" not in combined:
                score -= 5
            if score > best_score:
                best_score = score
                best_name = name
        return best_name

    def execute_with_retry(
        self,
        tool_name: str,
        payload: Dict[str, Any],
        selection_reason: str,
        dialect_handling: str,
        trace: List[Dict[str, Any]],
        max_retries: int = 2,
    ) -> Dict[str, Any]:
        attempts = 0
        latest_error: Optional[Dict[str, Any]] = None
        current_payload = dict(payload)
        while attempts <= max_retries:
            attempts += 1
            outcome = self.invoke_tool(tool_name, current_payload, selection_reason, dialect_handling, trace)
            if outcome.get("ok"):
                outcome["attempts"] = attempts
                return outcome
            latest_error = outcome
            current_payload = self._repair_payload(current_payload)
        return {
            "ok": False,
            "error": latest_error.get("error", "Unknown tool failure") if latest_error else "Unknown tool failure",
            "error_type": classify_failure(latest_error.get("error", ""), payload) if latest_error else "unknown_error",
            "sanitized_error": sanitize_error(latest_error.get("error", "")) if latest_error else "Execution failed.",
            "tool": tool_name,
            "failed_query": payload.get("sql") or payload.get("pipeline") or "",
            "attempts": attempts,
        }

    def invoke_tool(
        self,
        tool_name: str,
        payload: Dict[str, Any],
        selection_reason: str,
        dialect_handling: str,
        trace: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        started = time.time()
        response = self._mock_invoke_tool(tool_name, payload) if self.mock_mode else self._invoke_live(tool_name, payload)
        duration_ms = int((time.time() - started) * 1000)
        trace.append(
            {
                "tool_used": tool_name,
                "selection_reason": selection_reason,
                "dialect_handling": dialect_handling,
                "result_summary": result_summary(response.get("data") if response.get("ok") else sanitize_error(response.get("error", ""))),
                "duration_ms": duration_ms,
                "success": bool(response.get("ok")),
                "failure_type": None if response.get("ok") else classify_failure(response.get("error", ""), payload),
            }
        )
        if response.get("ok"):
            return response
        return {
            "ok": False,
            "error": response.get("error", "Tool invocation failed"),
            "error_type": classify_failure(response.get("error", ""), payload),
            "sanitized_error": sanitize_error(response.get("error", "")),
            "tool": tool_name,
            "failed_query": payload.get("sql") or payload.get("pipeline") or "",
        }

    def _invoke_live(self, tool_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        endpoint_variants = [
            f"{self.base_url}/v1/tools/{tool_name}:invoke",
            f"{self.base_url}/v1/tools/{tool_name}/invoke",
            f"{self.base_url}/v1/tools/invoke",
        ]
        body_variants = [
            {"arguments": payload},
            {"input": payload},
            {"tool": tool_name, "arguments": payload},
        ]
        last_error = "Unknown invocation error"
        for endpoint in endpoint_variants:
            for body in body_variants:
                try:
                    response = self.client.post(endpoint, json=body)
                    response.raise_for_status()
                    parsed = response.json()
                    return {"ok": True, "data": parsed}
                except Exception as exc:
                    last_error = str(exc)
        return {"ok": False, "error": last_error}

    def _repair_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        repaired = {}
        for key, value in payload.items():
            if isinstance(value, str):
                repaired[key] = value.strip()
            elif isinstance(value, list):
                repaired[key] = value
            else:
                repaired[key] = value
        return repaired

    def _parse_schema_payload(self, payload: Any) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {}
        if isinstance(payload, dict):
            for db_name, raw in payload.items():
                db = canonical_db_name(db_name)
                metadata.setdefault(db, {"tables": [], "collections": []})
                if isinstance(raw, dict):
                    for key in ["tables", "collections"]:
                        values = raw.get(key, [])
                        if isinstance(values, list):
                            metadata[db][key].extend(values)
        return metadata

    def _mock_tools_catalog(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": "postgres_primary_sql",
                "description": "PostgreSQL SQL execution tool for relational queries and joins.",
            },
            {
                "name": "mongodb_aggregate_pipeline",
                "description": "MongoDB aggregation pipeline executor for collections and nested fields.",
            },
            {
                "name": "sqlite_sql_runner",
                "description": "SQLite SQL execution tool for lightweight transaction datasets.",
            },
            {
                "name": "duckdb_sql_analytics",
                "description": "DuckDB SQL analytics execution for aggregations and window operations.",
            },
            {
                "name": "schema_discovery_global",
                "description": "Discover database schemas, table fields, and data types across configured connections.",
            },
        ]

    def _mock_schema_metadata(self) -> Dict[str, Any]:
        return {
            "postgresql": {
                "tables": [
                    {"name": "subscribers", "fields": {"subscriber_id": "INT", "monthly_revenue": "DECIMAL"}},
                    {"name": "business", "fields": {"business_id": "TEXT", "stars": "FLOAT"}},
                ],
                "collections": [],
            },
            "mongodb": {
                "tables": [],
                "collections": [
                    {"name": "support_tickets", "fields": {"customer_id": "STRING", "issue_description": "STRING", "ticket_count": "INT"}},
                    {"name": "subscribers", "fields": {"customer_id": "STRING", "plan_type": "STRING"}},
                ],
            },
            "sqlite": {
                "tables": [
                    {"name": "transactions", "fields": {"customer_id": "INTEGER", "amount": "REAL"}},
                ],
                "collections": [],
            },
            "duckdb": {
                "tables": [
                    {"name": "sales_fact", "fields": {"customer_id": "INTEGER", "total_sales": "DECIMAL"}},
                ],
                "collections": [],
            },
        }

    def _mock_invoke_tool(self, tool_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        text = str(payload.get("question", "")).lower()
        if payload.get("operation") == "schema_discovery":
            return {"ok": True, "data": self._mock_schema_metadata()}
        if "schema" in tool_name.lower() and "sql" not in tool_name.lower():
            return {"ok": True, "data": self._mock_schema_metadata()}
        if "force_error" in text:
            return {"ok": False, "error": "Forced mock error for validation path"}
        db = canonical_db_name(payload.get("database", ""))
        if db == "postgresql":
            return {
                "ok": True,
                "data": [
                    {"subscriber_id": 123, "monthly_revenue": 120.0, "plan_type": "postpaid"},
                    {"subscriber_id": 456, "monthly_revenue": 80.0, "plan_type": "prepaid"},
                ],
            }
        if db == "mongodb":
            return {
                "ok": True,
                "data": [
                    {
                        "customer_id": "CUST-123",
                        "ticket_count": 3,
                        "issue_description": "Customer is frustrated with service quality",
                    },
                    {
                        "customer_id": "CUST-456",
                        "ticket_count": 1,
                        "issue_description": "Customer says service is okay",
                    },
                ],
            }
        if db == "sqlite":
            return {
                "ok": True,
                "data": [
                    {"customer_id": 123, "amount": 220.5},
                    {"customer_id": 456, "amount": 80.0},
                ],
            }
        if db == "duckdb":
            return {
                "ok": True,
                "data": [
                    {"customer_id": 123, "total_sales": 1000.0},
                    {"customer_id": 456, "total_sales": 500.0},
                ],
            }
        return {"ok": False, "error": f"Unsupported mock database route for payload: {payload}"}
