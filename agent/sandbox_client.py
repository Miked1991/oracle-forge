from __future__ import annotations

from typing import Any, Callable, Dict, List


class SandboxClient:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled

    def execute_plan(
        self,
        plan: Dict[str, Any],
        step_executor: Callable[[Dict[str, Any]], Dict[str, Any]],
    ) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []
        validation_status = {"valid": True, "failed_steps": []}
        for step in plan.get("steps", []):
            try:
                outcome = step_executor(step)
            except Exception as exc:
                outcome = {"ok": False, "error": str(exc), "step_id": step.get("step_id")}
            results.append(outcome)
            if not outcome.get("ok"):
                validation_status["valid"] = False
                validation_status["failed_steps"].append(step.get("step_id"))
        return {
            "result": results,
            "trace": [{"sandbox_mode": "simulated", "steps_executed": len(results)}],
            "validation_status": validation_status,
        }
