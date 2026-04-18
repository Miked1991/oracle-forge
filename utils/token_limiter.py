from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List


@dataclass
class TokenUsage:
    prompt_tokens: int
    completion_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


class TokenLimiter:
    """Lightweight token budgeting helper for prompt safety and loop controls."""

    def __init__(self, max_prompt_tokens: int = 3500, max_tool_loops: int = 12) -> None:
        self.max_prompt_tokens = max_prompt_tokens
        self.max_tool_loops = max_tool_loops

    @staticmethod
    def estimate_tokens(text: str) -> int:
        if not text:
            return 0
        # Simple approximation: ~4 chars/token for English/code mix.
        return max(1, len(text) // 4)

    def truncate_text(self, text: str, target_tokens: int) -> str:
        if target_tokens <= 0:
            return ""
        max_chars = target_tokens * 4
        if len(text) <= max_chars:
            return text
        return text[: max_chars - 3] + "..."

    def trim_context_layers(self, layers: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Trim lower-priority context docs if prompt budget is exceeded."""
        serialized = self._serialize_layers(layers)
        if self.estimate_tokens(serialized) <= self.max_prompt_tokens:
            return layers

        # Priority: authoritative registry first, then runtime schema, domain, corrections.
        ordered = [
            "authoritative_registry",
            "schema_metadata",
            "domain_institutional",
            "interaction_memory",
            "v1_architecture",
            "v2_domain",
            "v3_corrections",
        ]
        remaining_tokens = self.max_prompt_tokens
        trimmed: Dict[str, Dict[str, str]] = {}

        for layer_name in ordered:
            docs = layers.get(layer_name)
            if not docs:
                continue
            trimmed[layer_name] = {}
            for rel_path, content in docs.items():
                budget = max(120, remaining_tokens // max(1, len(docs)))
                piece = self.truncate_text(content, budget)
                trimmed[layer_name][rel_path] = piece
                remaining_tokens -= self.estimate_tokens(piece)
                if remaining_tokens <= 0:
                    return trimmed

        for layer_name, docs in layers.items():
            if layer_name in trimmed:
                continue
            trimmed[layer_name] = {}
            for rel_path, content in docs.items():
                budget = max(80, remaining_tokens // max(1, len(docs)))
                piece = self.truncate_text(content, budget)
                trimmed[layer_name][rel_path] = piece
                remaining_tokens -= self.estimate_tokens(piece)
                if remaining_tokens <= 0:
                    return trimmed
        return trimmed

    def enforce_loop_limit(self, loop_count: int) -> bool:
        return loop_count <= self.max_tool_loops

    def usage_entry(self, prompt_text: str, completion_text: str = "") -> Dict[str, Any]:
        usage = TokenUsage(
            prompt_tokens=self.estimate_tokens(prompt_text),
            completion_tokens=self.estimate_tokens(completion_text),
        )
        return {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
            "max_prompt_tokens": self.max_prompt_tokens,
            "max_tool_loops": self.max_tool_loops,
        }

    @staticmethod
    def _serialize_layers(layers: Dict[str, Dict[str, str]]) -> str:
        parts: List[str] = []
        for layer, docs in layers.items():
            parts.append(f"## {layer}")
            for rel, content in docs.items():
                parts.append(rel)
                parts.append(content)
        return "\n".join(parts)

