from __future__ import annotations

from typing import Any, Dict, List

from agent.main import run_agent


def dab_entry(question: str, available_databases: List[str], schema_info: Dict[str, Any]) -> Dict[str, Any]:
    return run_agent(question=question, available_databases=available_databases, schema_info=schema_info)
