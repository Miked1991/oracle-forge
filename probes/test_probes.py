#!/usr/bin/env python3
"""
test_probes.py - Automated Adversarial Probe Runner for Oracle Forge Agent

Runs 21 probes against the data agent and verifies correctness.
Probe definitions are kept in sync with probes/probes.md (v3.0).

Usage:
    # Test a live agent via HTTP endpoint
    python test_probes.py --agent-url http://localhost:8000

    # Test the agent class directly (requires agent module on PYTHONPATH)
    python test_probes.py --direct

    # Run only specific probes by ID
    python test_probes.py --agent-url http://localhost:8000 --probes M1,J2,U3

Results are saved to test_results.json alongside this file.
"""

import asyncio
import json
import sys
import os
import re
import argparse
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

# Max probes executing simultaneously. Keeps the agent from being overwhelmed
# while still running the 21-probe suite in parallel.
_PROBE_CONCURRENCY = 5

# Agent query timeout in seconds (cross-DB joins can be slow).
_QUERY_TIMEOUT = 60.0

# -----------------------------------------------------------------------------
# Probe Definitions — must stay in sync with probes.md Quick Reference Table.
#
# Each entry: (id, category, query, expected_keywords)
#
# Keyword matching contract:
#   - Single-word keyword  → word-boundary match  (prevents 'high' → 'highway')
#   - Multi-word phrase    → case-insensitive substring match
# -----------------------------------------------------------------------------
PROBES: List[Tuple[str, str, str, List[str]]] = [
    # ── M: Multi-database routing (6 probes) ─────────────────────────────────
    (
        "M1", "M",
        "Which customers have both made a purchase in Q3 2024 AND have an open "
        "support ticket?",
        # Correct response joins PostgreSQL (orders) with DuckDB (tickets).
        # Must mention customers matched across both sources.
        ["Q3 2024", "support ticket"],
    ),
    (
        "M2", "M",
        "What is the average review rating for businesses that have more than "
        "100 check-ins?",
        # Must recompute from MongoDB reviews.stars, NOT use stale business.stars.
        # Correct response names a rating value derived from live review data.
        ["rating", "check-ins"],
    ),
    (
        "M3", "M",
        "List the top 5 cities by number of new customers acquired in Q1 2024.",
        # Negative probe — PostgreSQL only (customers.city, customers.created_at).
        # Agent must NOT fan out to DuckDB.
        ["cities", "Q1 2024"],
    ),
    (
        "M4", "M",
        "For each GitHub repository, how many unique contributors also appear "
        "in the dependency graph?",
        # Requires DuckDB (repos, contributors) + SQLite (dependencies).
        # Agent must execute per-engine, then merge in Python.
        ["contributors", "dependency"],
    ),
    (
        "M5", "M",
        "Which patients have both a high-risk mutation AND low gene expression "
        "for the TP53 gene?",
        # Requires DuckDB (gene_expression) + PostgreSQL (mutations).
        # patient_id format mismatch: 'TCGA-XX-XXXX' vs UUID — resolve_tcga_id().
        ["TP53", "mutation"],
    ),
    (
        "M6", "M",
        "Join the SQLite `customers` table (customer_id TEXT like 'ID-98765') "
        "with DuckDB `loyalty` table (cust_id INTEGER) using the first 5 digits "
        "of the numeric part. Return name and loyalty points.",
        # resolve_chain(['strip_prefix', 'first_5_chars']) + zfill(5) for short keys.
        ["name", "points"],
    ),

    # ── J: Ill-formatted join keys (5 probes) ────────────────────────────────
    (
        "J1", "J",
        "How many customers in the CRM database have a churn risk score "
        "above 0.7?",
        # customer_id (PostgreSQL int) vs customer_ref ('C{id}' string in DuckDB).
        # Naive join returns 0 rows; fix strips 'C' prefix before joining.
        ["churn risk", "0.7"],
    ),
    (
        "J2", "J",
        "What is the average NPS score for enterprise-tier customers?",
        # Negative probe — int-to-int join (PostgreSQL + SQLite).
        # Agent must NOT add unnecessary prefix stripping.
        ["NPS", "enterprise"],
    ),
    (
        "J3", "J",
        "Which books have reviews with an average rating below 3 in both "
        "the PostgreSQL and SQLite databases?",
        # Negative probe — integer book_id in both DBs; direct join, no transform.
        ["books", "rating"],
    ),
    (
        "J4", "J",
        "Find customers who have tickets labeled 'CUST-0001001' in the DuckDB "
        "system but appear as ID 1001 in the PostgreSQL system.",
        # strip_cust_prefix(): 'CUST-0001001' → 1001 (zero-pad lookahead prevents
        # ValueError on all-zero IDs like 'CUST-0000000').
        ["CUST-0001001", "1001"],
    ),
    (
        "J5", "J",
        "How many Yelp business reviews have a 'useful' vote count above the "
        "median for that business category?",
        # DuckDB business.categories is pipe-separated ('Restaurants|Pizza|Italian').
        # Must split on '|' for exact category match; business_id join is direct.
        ["useful", "median"],
    ),

    # ── U: Unstructured text extraction (4 probes) ───────────────────────────
    (
        "U1", "U",
        "How many Yelp reviews in 2024 mention 'wait time' as a negative "
        "experience?",
        # LIKE '%wait%' over-counts by 3-4x (includes 'can't wait', 'wait staff').
        # Fix: WAIT_COMPLAINT regex scopes to complaint phrases only.
        ["wait time", "2024"],
    ),
    (
        "U2", "U",
        "What percentage of support tickets in Q4 2023 mention the word "
        "'urgent' in their description?",
        # Case-sensitive LIKE '%urgent%' misses 'Urgent' and 'URGENT' (~15% undercount).
        # Fix: ILIKE in PostgreSQL or str.contains(case=False) in Python.
        ["urgent", "Q4 2023"],
    ),
    (
        "U3", "U",
        "Classify the top 10 most-reviewed Yelp businesses by whether their "
        "review text is predominantly positive or negative.",
        # Agent must call classify_bulk() BEFORE return_answer.
        # Three-step pipeline: top-10 IDs (DuckDB) → texts (MongoDB) → classify.
        ["positive", "negative"],
    ),
    (
        "U4", "U",
        "How many GitHub repositories have a README that mentions "
        "'machine learning' or 'deep learning'?",
        # NULL guard required: readme_text IS NOT NULL before LIKE filter.
        # Without it, NULL rows cause silent miscounts on sparse datasets.
        ["machine learning", "deep learning"],
    ),

    # ── D: Domain knowledge (6 probes) ───────────────────────────────────────
    (
        "D1", "D",
        "Which customers are currently 'active' in the CRM system?",
        # 'Active' is NOT row existence. Definition: at least one purchase in the
        # last 90 days. Filter: WHERE last_purchase_date >= CURRENT_DATE - 90 days.
        ["90 days", "active"],
    ),
    (
        "D2", "D",
        "What was the total revenue for Q3 2023, excluding refunded orders?",
        # Must filter: status NOT IN ('refunded', 'cancelled', 'returned').
        # Never sum full orders table for revenue figures.
        ["revenue", "refund"],
    ),
    (
        "D3", "D",
        "What is the daily return for AAPL stock in the week of 2024-01-15?",
        # Must use close-to-close pct_change(), NOT intraday (close/open - 1).
        # First row is NaN — do not fill with 0.
        ["daily return", "AAPL"],
    ),
    (
        "D4", "D",
        "Which Yelp businesses are currently open and have a rating of 4.5 "
        "or higher?",
        # Must include is_open filter; rating alone is insufficient.
        # Without is_open, permanently closed businesses appear in results.
        # Keyword "open" is used instead of "is_open" so correct answers that
        # phrase the filter as "currently open" or "open businesses" still pass.
        ["open", "4.5"],
    ),
    (
        "D5", "D",
        "What is the NPS promoter zone threshold for crmarenapro?",
        # crmarenapro uses -100 to +100 scale (NOT standard 0-10).
        # Promoter zone = score >= 50. Do NOT apply standard NPS logic.
        ["50", "promoter"],
    ),
    (
        "D6", "D",
        "Show total sales for FY2025. Use the authoritative revenue source.",
        # FY2025 = July 1 2024 - June 30 2025 (not calendar year).
        # Authoritative table: finance.fact_revenue (NOT deprecated sales.order_line).
        # "fact_revenue" replaced with "revenue" so answers that reference the
        # table as "fact revenue table" or "the revenue fact" also pass;
        # "authoritative" catches answers that explicitly name the correct source.
        ["FY2025", "revenue", "authoritative"],
    ),
]

# Category labels for the summary report
_CAT_LABELS: Dict[str, str] = {
    "M": "Multi-DB Routing   ",
    "J": "Ill-Formatted Keys ",
    "U": "Unstructured Text  ",
    "D": "Domain Knowledge   ",
}


class ProbeTestRunner:
    """
    Adversarial probe runner for Oracle Forge.

    Supports two modes:
    1. HTTP   — POSTs queries to a live agent REST endpoint (/ask)
    2. Direct — imports DataAgent locally and calls answer() directly

    Probes run concurrently (bounded by _PROBE_CONCURRENCY). Each answer is
    checked against expected keywords using word-boundary matching for single
    words and substring matching for phrases.
    """

    def __init__(
        self,
        agent_url: Optional[str] = None,
        use_direct: bool = False,
    ) -> None:
        self.agent_url = agent_url
        self.use_direct = use_direct
        self.agent: Optional[Any] = None       # DataAgent instance (direct mode)
        self.session: Optional[Any] = None     # aiohttp.ClientSession (HTTP mode)

    # -------------------------------------------------------------------------
    # Initialisation
    # -------------------------------------------------------------------------
    async def init(self) -> None:
        """Set up HTTP session or import and initialise the agent."""
        if self.use_direct:
            try:
                from agent.core.agent import DataAgent  # type: ignore[import]
                self.agent = DataAgent()
                assert self.agent is not None
                await self.agent.init()
                print("[Direct mode] Agent initialised successfully.")
            except ImportError as exc:
                print(f"ERROR: Could not import DataAgent: {exc}")
                print(
                    "Expected layout: agent/core/agent.py  →  class DataAgent\n"
                    "Ensure the agent package is on PYTHONPATH, or use --agent-url."
                )
                sys.exit(1)

        elif self.agent_url:
            try:
                import aiohttp
                self.session = aiohttp.ClientSession()
                async with self.session.get(
                    f"{self.agent_url}/health",
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        print(
                            f"ERROR: Agent health check returned HTTP {resp.status}. "
                            f"Is the agent running at {self.agent_url}?"
                        )
                        await self.session.close()
                        sys.exit(1)
                print(f"[HTTP mode] Agent reachable at {self.agent_url}")
            except ImportError:
                print("ERROR: aiohttp not installed. Run: pip install aiohttp")
                sys.exit(1)
            except Exception as exc:
                print(f"ERROR: Could not reach agent at {self.agent_url}: {exc}")
                sys.exit(1)
        else:
            print("ERROR: Must specify --agent-url or --direct")
            sys.exit(1)

    async def close(self) -> None:
        """Release HTTP session."""
        if self.session:
            await self.session.close()

    # -------------------------------------------------------------------------
    # Keyword matching
    # -------------------------------------------------------------------------
    @staticmethod
    def _keyword_found(keyword: str, text: str) -> bool:
        """
        Case-insensitive match with anti-false-positive guard.

        Single-word keyword  → word-boundary regex  (\\bkeyword\\b)
            Prevents 'high' matching 'highway', 'actions' matching 'transactions'.
        Multi-word phrase    → plain substring match
            'total sales', 'active accounts', 'churn rate', etc.
        """
        if " " in keyword:
            return keyword.lower() in text.lower()
        return bool(re.search(r"\b" + re.escape(keyword) + r"\b", text, re.IGNORECASE))

    # -------------------------------------------------------------------------
    # Query dispatch
    # -------------------------------------------------------------------------
    async def ask_agent(self, query: str) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Send one query to the agent. Returns (answer_text, trace_dict | None).

        Both modes enforce _QUERY_TIMEOUT. Direct mode previously had no timeout,
        which would hang the entire suite on a stalled multi-DB query.
        """
        if self.use_direct:
            assert self.agent is not None, "agent not initialised — call init() first"
            result: Dict[str, Any] = await asyncio.wait_for(
                self.agent.answer(query), timeout=_QUERY_TIMEOUT
            )
            return result.get("answer", ""), result.get("trace")

        if self.agent_url:
            assert self.session is not None, "session not initialised — call init() first"
            import aiohttp
            async with self.session.post(
                f"{self.agent_url}/ask",
                json={"question": query},
                timeout=aiohttp.ClientTimeout(total=_QUERY_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    return f"HTTP Error {resp.status}: {error_text}", None
                data = await resp.json()
                return data.get("answer", ""), data.get("trace")

        raise RuntimeError("No agent connection configured")

    # -------------------------------------------------------------------------
    # Single probe execution
    # -------------------------------------------------------------------------
    async def run_single_probe(
        self,
        probe_id: str,
        query: str,
        expected_keywords: List[str],
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Execute one probe. Returns (passed, message, trace).
        """
        try:
            answer, trace = await self.ask_agent(query)
            missing = [
                kw for kw in expected_keywords
                if not self._keyword_found(kw, answer)
            ]
            if not missing:
                return True, "All keywords found", trace
            return False, f"Missing keywords: {missing}", trace

        except asyncio.TimeoutError:
            return False, f"[{probe_id}] Timeout: no response within {_QUERY_TIMEOUT:.0f}s", None
        except Exception as exc:
            return False, f"[{probe_id}] Exception: {str(exc)[:150]}", None

    # -------------------------------------------------------------------------
    # Orchestration
    # -------------------------------------------------------------------------
    async def run_all(self, probe_filter: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run all probes concurrently and return a results dictionary.

        Probes execute in parallel up to _PROBE_CONCURRENCY at a time.
        Output is printed as each probe completes; the final summary is
        presented in original probe order.
        """
        probes_to_run = PROBES
        if probe_filter:
            filter_set = set(probe_filter)
            probes_to_run = [p for p in PROBES if p[0] in filter_set]

        total = len(probes_to_run)
        order = {pid: i for i, (pid, *_) in enumerate(probes_to_run)}

        print(f"\n{'='*60}")
        print(f"Running {total} adversarial probes  (concurrency={_PROBE_CONCURRENCY})")
        if probe_filter:
            print(f"Filter: {', '.join(probe_filter)}")
        print(f"{'='*60}\n")

        semaphore = asyncio.Semaphore(_PROBE_CONCURRENCY)

        async def _run(pid: str, cat: str, query: str, keywords: List[str]) -> Dict[str, Any]:
            async with semaphore:
                passed, message, trace = await self.run_single_probe(pid, query, keywords)
            status_str = "✅ PASS" if passed else f"❌ FAIL – {message}"
            print(f"  [{pid}] [{cat}] {status_str}")
            return {
                "id": pid,
                "category": cat,
                "query": query[:200],
                "expected_keywords": keywords,
                "passed": passed,
                "message": message,
                "trace_snippet": (
                    json.dumps(trace, default=str)[:300] if trace else None
                ),
                "timestamp": datetime.now().isoformat(),
            }

        raw: List[Dict[str, Any]] = await asyncio.gather(*[
            _run(pid, cat, query, kws)
            for pid, cat, query, kws in probes_to_run
        ])

        results: List[Dict[str, Any]] = sorted(raw, key=lambda r: order[r["id"]])
        passed_count = sum(1 for r in results if r["passed"])

        # ── Overall summary ────────────────────────────────────────────────
        pass_rate = (passed_count / total * 100) if total else 0.0
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Passed : {passed_count}/{total}")
        print(f"Failed : {total - passed_count}/{total}")
        print(f"Pass % : {pass_rate:.1f}%")

        # ── Per-category breakdown ─────────────────────────────────────────
        cat_stats: Dict[str, Dict[str, Any]] = {}
        for r in results:
            cat = r["category"]
            if cat not in cat_stats:
                cat_stats[cat] = {"passed": 0, "total": 0}
            cat_stats[cat]["total"] += 1
            if r["passed"]:
                cat_stats[cat]["passed"] += 1

        print("\nCategory Breakdown:")
        for cat in ["M", "J", "U", "D"]:
            if cat not in cat_stats:
                continue
            s = cat_stats[cat]
            rate = (s["passed"] / s["total"]) * 100
            bar = "█" * s["passed"] + "░" * (s["total"] - s["passed"])
            print(f"  {_CAT_LABELS[cat]}: {s['passed']}/{s['total']}  ({rate:5.1f}%)  [{bar}]")

        # ── Persist to file ────────────────────────────────────────────────
        output = {
            "timestamp": datetime.now().isoformat(),
            "mode": "direct" if self.use_direct else f"http:{self.agent_url}",
            "total_probes": total,
            "passed": passed_count,
            "failed": total - passed_count,
            "pass_rate": pass_rate,
            "category_breakdown": {
                cat: {
                    "passed": cat_stats[cat]["passed"],
                    "total": cat_stats[cat]["total"],
                    "pass_rate": (
                        cat_stats[cat]["passed"] / cat_stats[cat]["total"] * 100
                    ),
                }
                for cat in cat_stats
            },
            "results": results,
        }

        output_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test_results.json"
        )
        with open(output_file, "w") as fh:
            json.dump(output, fh, indent=2)
        print(f"\nDetailed results saved to: {output_file}")

        return output


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------
async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run adversarial probes against Oracle Forge agent"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--agent-url",
        type=str,
        help="Base URL of live agent (e.g., http://localhost:8000)",
    )
    group.add_argument(
        "--direct",
        action="store_true",
        help="Import DataAgent directly (requires agent package on PYTHONPATH)",
    )
    parser.add_argument(
        "--probes",
        type=str,
        help="Comma-separated probe IDs to run (e.g., M1,J2,U3). Default: all",
    )

    args = parser.parse_args()

    probe_filter: Optional[List[str]] = None
    if args.probes:
        probe_filter = [p.strip() for p in args.probes.split(",")]

    runner = ProbeTestRunner(agent_url=args.agent_url, use_direct=args.direct)
    try:
        await runner.init()
        output = await runner.run_all(probe_filter=probe_filter)
        sys.exit(1 if output["failed"] > 0 else 0)
    finally:
        await runner.close()


if __name__ == "__main__":
    asyncio.run(main())
