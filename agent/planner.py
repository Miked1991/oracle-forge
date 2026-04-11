from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from .utils import canonical_db_name


@dataclass
class PlanStep:
    step_id: int
    database: str
    objective: str
    selection_reason: str
    dialect: str
    query_payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step_id": self.step_id,
            "database": self.database,
            "objective": self.objective,
            "selection_reason": self.selection_reason,
            "dialect": self.dialect,
            "query_payload": self.query_payload,
        }


class QueryPlanner:
    def __init__(self, context: Dict[str, Any]) -> None:
        self.context = context

    def create_plan(
        self,
        question: str,
        available_databases: List[str],
    ) -> Dict[str, Any]:
        question_l = question.lower()
        available = [canonical_db_name(item) for item in available_databases]
        selected = self._select_databases(question_l, available)
        steps: List[PlanStep] = []
        for index, db in enumerate(selected, start=1):
            dialect = "mongodb_aggregation" if db == "mongodb" else "sql"
            payload = self._build_query_payload(question_l, db, dialect)
            steps.append(
                PlanStep(
                    step_id=index,
                    database=db,
                    objective=f"Fetch relevant evidence from {db}",
                    selection_reason=self._selection_reason(question_l, db),
                    dialect=dialect,
                    query_payload=payload,
                )
            )
        return {
            "question": question,
            "plan_type": "multi_db" if len(steps) > 1 else "single_db",
            "requires_join": len(steps) > 1 or "join" in question_l or "correlate" in question_l,
            "steps": [step.to_dict() for step in steps],
        }

    def _select_databases(self, question: str, available: List[str]) -> List[str]:
        picks: List[str] = []
        rulebook = {
            "postgresql": ["sql", "subscriber", "business", "review", "relational", "table"],
            "mongodb": ["mongo", "document", "ticket", "issue", "sentiment", "aggregation", "pipeline"],
            "sqlite": ["sqlite", "transaction", "inventory", "store"],
            "duckdb": ["duckdb", "analytics", "window", "trend", "cube", "aggregate"],
        }
        for db, keywords in rulebook.items():
            if db in available and any(keyword in question for keyword in keywords):
                picks.append(db)
        if ("join" in question or "correlate" in question or "across" in question) and "mongodb" in available and "postgresql" in available:
            if "postgresql" not in picks:
                picks.append("postgresql")
            if "mongodb" not in picks:
                picks.append("mongodb")
        if not picks and available:
            priority = ["postgresql", "mongodb", "sqlite", "duckdb"]
            for candidate in priority:
                if candidate in available:
                    picks.append(candidate)
                    break
        ordered: List[str] = []
        for candidate in ["postgresql", "mongodb", "sqlite", "duckdb"]:
            if candidate in picks and candidate not in ordered:
                ordered.append(candidate)
        return ordered

    def _selection_reason(self, question: str, db: str) -> str:
        if db == "mongodb":
            return "MongoDB selected for document-oriented or aggregation intent and nested fields."
        if db == "postgresql":
            return "PostgreSQL selected as primary SQL source with strongest relational coverage."
        if db == "sqlite":
            return "SQLite selected for lightweight transactional queries."
        if db == "duckdb":
            return "DuckDB selected for analytical aggregate processing."
        return f"{db} selected based on routing heuristics."

    def _build_query_payload(self, question: str, db: str, dialect: str) -> Dict[str, Any]:
        schema = self.context.get("schema_metadata", {}).get(db, {})
        if db == "mongodb":
            collection = self._first_name(schema.get("collections"), "support_tickets")
            pipeline: List[Dict[str, Any]] = [{"$limit": 100}]
            if "count" in question:
                pipeline = [
                    {"$limit": 100},
                    {"$group": {"_id": None, "count": {"$sum": 1}}},
                ]
            return {
                "database": db,
                "dialect": dialect,
                "collection": collection,
                "pipeline": pipeline,
                "question": question,
            }
        table = self._first_name(schema.get("tables"), "subscribers")
        sql = f"SELECT * FROM {table} LIMIT 100"
        if "count" in question:
            sql = f"SELECT COUNT(*) AS count FROM {table}"
        return {
            "database": db,
            "dialect": dialect,
            "sql": sql,
            "question": question,
        }

    @staticmethod
    def _first_name(collection: Any, fallback: str) -> str:
        if isinstance(collection, list) and collection:
            first = collection[0]
            if isinstance(first, dict) and "name" in first:
                return first["name"]
            if isinstance(first, str):
                return first
        return fallback
