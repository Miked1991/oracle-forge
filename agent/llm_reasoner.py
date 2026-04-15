from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import httpx

from utils.token_limiter import TokenLimiter

try:
    from groq import Groq
except Exception:  # pragma: no cover - optional runtime dependency
    Groq = None  # type: ignore[assignment]


@dataclass
class LLMGuidance:
    selected_databases: List[str]
    rationale: str
    query_hints: Dict[str, Any]
    model: str
    used_llm: bool


class GroqLlamaReasoner:
    def __init__(self, repo_root: Optional[Path] = None, token_limiter: Optional[TokenLimiter] = None) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[1]
        load_dotenv(self.repo_root / ".env", override=False)
        self.groq_api_key = self._clean_env("GROQ_API_KEY")
        self.openrouter_api_key = self._clean_env("OPENROUTER_API_KEY")
        self.provider = self._resolve_provider()
        self.model_name = self._resolve_model_name()
        self.openrouter_base_url = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").strip().rstrip("/")
        self.openrouter_site_url = os.getenv("OPENROUTER_SITE_URL", "").strip()
        self.openrouter_app_name = os.getenv("OPENROUTER_APP_NAME", "").strip()
        self.token_limiter = token_limiter or TokenLimiter()
        self.client = Groq(api_key=self.groq_api_key) if self.provider == "groq" and self.groq_api_key and Groq is not None else None
        self.http_client = httpx.Client(timeout=40)

    def plan(self, question: str, available_databases: List[str], context: Dict[str, Any]) -> LLMGuidance:
        fallback = self._fallback(question, available_databases)
        if self.provider == "groq" and self.client is None:
            return fallback

        context_layers = context.get("context_layers", {})
        trimmed_layers = self.token_limiter.trim_context_layers(context_layers)
        prompt = self._build_prompt(question, available_databases, trimmed_layers)
        prompt = self.token_limiter.truncate_text(prompt, self.token_limiter.max_prompt_tokens)

        try:
            if self.provider == "openrouter":
                payload = self._plan_with_openrouter(prompt)
            else:
                payload = self._plan_with_groq(prompt)
            if not isinstance(payload, dict):
                return fallback
            selected = payload.get("selected_databases", [])
            if not isinstance(selected, list):
                selected = []
            selected_norm = [str(item).strip().lower() for item in selected if str(item).strip()]
            filtered = [db for db in selected_norm if db in [d.lower() for d in available_databases]]
            if not filtered:
                filtered = fallback.selected_databases
            return LLMGuidance(
                selected_databases=filtered,
                rationale=str(payload.get("rationale", "LLM-guided routing."))[:500],
                query_hints=payload.get("query_hints", {}) if isinstance(payload.get("query_hints", {}), dict) else {},
                model=self.model_name,
                used_llm=True,
            )
        except Exception:
            return fallback

    def _plan_with_groq(self, prompt: str) -> Dict[str, Any]:
        if self.client is None:
            raise RuntimeError("Groq client is unavailable.")
        response = self.client.chat.completions.create(
            model=self.model_name,
            temperature=0,
            max_tokens=320,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a database routing and query planning assistant for a multi-DB data agent. "
                        "Return strict JSON with keys: selected_databases, rationale, query_hints."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
        content = (response.choices[0].message.content or "{}").strip()
        return self._parse_json_content(content)

    def _plan_with_openrouter(self, prompt: str) -> Dict[str, Any]:
        if not self.openrouter_api_key:
            raise RuntimeError("OPENROUTER_API_KEY is missing.")
        headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json",
        }
        if self.openrouter_site_url:
            headers["HTTP-Referer"] = self.openrouter_site_url
        if self.openrouter_app_name:
            headers["X-Title"] = self.openrouter_app_name

        body = {
            "model": self.model_name,
            "temperature": 0,
            "max_tokens": 320,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a database routing and query planning assistant for a multi-DB data agent. "
                        "Return strict JSON with keys: selected_databases, rationale, query_hints."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        response = self.http_client.post(f"{self.openrouter_base_url}/chat/completions", headers=headers, json=body)
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices", [])
        if not choices:
            raise RuntimeError("OpenRouter returned no choices.")
        message = choices[0].get("message", {})
        content = message.get("content", "{}")
        if isinstance(content, list):
            content = "".join(str(part.get("text", "")) for part in content if isinstance(part, dict))
        return self._parse_json_content(str(content).strip())

    @staticmethod
    def _parse_json_content(content: str) -> Dict[str, Any]:
        text = content.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:].strip()
        parsed = json.loads(text or "{}")
        return parsed if isinstance(parsed, dict) else {}

    def _resolve_provider(self) -> str:
        configured = os.getenv("LLM_PROVIDER", "").strip().lower()
        if configured in {"groq", "openrouter"}:
            return configured
        if self.openrouter_api_key:
            return "openrouter"
        return "groq"

    def _resolve_model_name(self) -> str:
        configured = os.getenv("MODEL_NAME", "").strip()
        if configured:
            return configured
        if self.provider == "openrouter":
            return "openai/gpt-4o-mini"
        return "llama-3.3-70b-versatile"

    @staticmethod
    def _clean_env(name: str) -> str:
        value = os.getenv(name, "").strip()
        if not value:
            return ""
        lowered = value.lower()
        if lowered in {"your_api_key_here", "your_key_here", "changeme"}:
            return ""
        if lowered.startswith("your_") and ("_key_here" in lowered or "_api_key_here" in lowered):
            return ""
        return value

    def _build_prompt(self, question: str, available_databases: List[str], context_layers: Dict[str, Any]) -> str:
        context_json = json.dumps(context_layers, ensure_ascii=False)[:12000]
        return (
            f"Question: {question}\n"
            f"Available databases: {available_databases}\n"
            "Use the provided context to choose database routes and query hints for each DB.\n"
            "Context layers (trimmed):\n"
            f"{context_json}\n"
            "Return JSON only."
        )

    def _fallback(self, question: str, available_databases: List[str]) -> LLMGuidance:
        question_l = question.lower()
        picks: List[str] = []
        for db in ["duckdb", "mongodb", "postgresql", "sqlite"]:
            if db in [d.lower() for d in available_databases] and db in question_l:
                picks.append(db)
        if not picks:
            for db in ["duckdb", "mongodb", "postgresql", "sqlite"]:
                if db in [d.lower() for d in available_databases]:
                    picks.append(db)
                    break
        if any(token in question_l for token in ["join", "across", "both", "combine"]) and "mongodb" in [d.lower() for d in available_databases]:
            if "mongodb" not in picks:
                picks.append("mongodb")
        return LLMGuidance(
            selected_databases=picks,
            rationale="Fallback routing used due unavailable LLM response.",
            query_hints={},
            model=self.model_name,
            used_llm=False,
        )

