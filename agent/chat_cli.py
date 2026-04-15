"""
Interactive CLI for Oracle Forge — real agent + MCP + databases.

Uses optional multi-turn `conversation_history` (same as `run_agent(..., conversation_history=...)`).
Eval harness is unchanged: `eval/run_dab_eval.py` does not pass history.

Answers are printed in plain language only (no query traces). If the agent returns raw
row dumps (e.g. generic LIMIT queries), you get a short notice instead of JSON.

Usage (from repo root, with `.env` and MCP up):

    python -m agent.chat_cli
    python -m agent.chat_cli --dbs postgresql
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict, List

from agent.user_facing_format import format_answer_plain

# Cap stored turns so prompts stay bounded (each turn = user + assistant message).
_MAX_HISTORY_MESSAGES = 24


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive Oracle Forge query CLI (plain answers only)")
    parser.add_argument(
        "--dbs",
        default="postgresql,mongodb,sqlite,duckdb",
        help="Comma-separated backends this session may use (same as agent.main --dbs)",
    )
    args = parser.parse_args()
    databases = [x.strip() for x in args.dbs.split(",") if x.strip()]

    from agent.main import run_agent  # noqa: PLC0415

    history: List[Dict[str, str]] = []

    print("Oracle Forge — type a question (empty line, /q, or exit to stop).\n")

    while True:
        try:
            line = input("oracle-forge> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not line or line.lower() in {"/q", "/quit", "exit", "quit"}:
            break

        result = run_agent(
            line,
            databases,
            {},
            conversation_history=history or None,
        )
        print(format_answer_plain(result))
        print()

        history.append({"role": "user", "content": line})
        history.append({"role": "assistant", "content": format_answer_plain(result)})
        if len(history) > _MAX_HISTORY_MESSAGES:
            history = history[-_MAX_HISTORY_MESSAGES:]


if __name__ == "__main__":
    main()
    sys.exit(0)
