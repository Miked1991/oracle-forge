"""
Interactive CLI for Oracle Forge — real agent + MCP + databases.

Uses optional multi-turn `conversation_history` (same as `run_agent(..., conversation_history=...)`).
Eval harness is unchanged: `eval/run_dab_eval.py` does not pass history.

Answers are printed in plain language only (no query traces). If the agent returns raw
row dumps (e.g. generic LIMIT queries), you get a short notice instead of JSON.

Usage (from repo root, with `.env` and MCP up):

    python -m agent.chat_cli
    python -m agent.chat_cli --dbs postgresql,mongodb,duckdb
    python -m agent.chat_cli --dataset-id yelp --dbs postgresql,mongodb,duckdb
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

from agent.user_facing_format import format_answer_plain

_REPO_ROOT = Path(__file__).resolve().parents[1]

# Cap stored turns so prompts stay bounded (each turn = user + assistant message).
_MAX_HISTORY_MESSAGES = 24


def main() -> None:
    load_dotenv(_REPO_ROOT / ".env", override=True)
    parser = argparse.ArgumentParser(description="Interactive Oracle Forge query CLI (plain answers only)")
    parser.add_argument(
        "--dbs",
        default="postgresql,mongodb,sqlite,duckdb",
        help="Comma-separated backends this session may use (same as eval/query_agent_concise --dbs)",
    )
    parser.add_argument(
        "--dataset-id",
        default="",
        metavar="KEY",
        help="Optional dataset profile key (e.g. yelp); same as run_agent dataset_id / eval --dataset-id",
    )
    args = parser.parse_args()
    databases = [x.strip() for x in args.dbs.split(",") if x.strip()]
    dataset_id: Optional[str] = (args.dataset_id or "").strip() or None

    from agent.main import run_agent  # noqa: PLC0415

    history: List[Dict[str, str]] = []

    ds_note = f" [dataset_id={dataset_id}]" if dataset_id else ""
    print(f"Oracle Forge — type a question (empty line, /q, or exit to stop).{ds_note}\n")

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
            dataset_id=dataset_id,
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
