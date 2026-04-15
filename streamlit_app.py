"""
Oracle Forge — Streamlit UI for natural-language queries (same engine as CLI / eval).

Run locally (repo root, MCP + `.env` ready):

    streamlit run streamlit_app.py

Docker: see README "Streamlit UI (web)" and `docker compose --profile ui`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Do not override existing env (e.g. MCP_BASE_URL=http://toolbox:5000 from Docker Compose).
load_dotenv(ROOT / ".env", override=False)

from agent.main import run_agent  # noqa: E402
from agent.user_facing_format import format_answer_plain  # noqa: E402

_MAX_HISTORY_MESSAGES = 24
_DEFAULT_DBS = "postgresql,mongodb,sqlite,duckdb"


def _parse_dbs(text: str) -> List[str]:
    return [x.strip() for x in text.split(",") if x.strip()]


def main() -> None:
    st.set_page_config(page_title="Oracle Forge", page_icon="🔮", layout="centered")
    st.title("Oracle Forge")
    st.caption("Multi-database analytics agent — answers only; traces stay on the server.")

    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "conversation_history" not in st.session_state:
        st.session_state.conversation_history = []

    with st.sidebar:
        st.header("Settings")
        default_dbs = os.getenv("ORACLE_FORGE_STREAMLIT_DBS", _DEFAULT_DBS)
        dbs_text = st.text_input(
            "Available databases (comma-separated)",
            value=default_dbs,
            help="Same as CLI `--dbs`. Use `postgresql` alone for more stable Yelp SQL templates.",
        )
        if st.button("Clear conversation"):
            st.session_state.messages = []
            st.session_state.conversation_history = []
            st.rerun()

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    prompt = st.chat_input("Ask about your connected databases…")
    if not prompt:
        return

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    databases = _parse_dbs(dbs_text)
    with st.chat_message("assistant"):
        with st.spinner("Running agent…"):
            try:
                result: Dict[str, Any] = run_agent(
                    prompt,
                    databases,
                    {},
                    conversation_history=st.session_state.conversation_history or None,
                )
                text = format_answer_plain(result)
            except Exception as exc:  # noqa: BLE001 — show user-friendly error in UI
                text = f"Error: {exc}"

        st.markdown(text)

    st.session_state.messages.append({"role": "assistant", "content": text})
    st.session_state.conversation_history.append({"role": "user", "content": prompt})
    st.session_state.conversation_history.append({"role": "assistant", "content": text})
    if len(st.session_state.conversation_history) > _MAX_HISTORY_MESSAGES:
        st.session_state.conversation_history = st.session_state.conversation_history[
            -_MAX_HISTORY_MESSAGES:
        ]


if __name__ == "__main__":
    main()
