#!/usr/bin/env python3
"""
Injection Test Script for Knowledge Base Documents
Tests each KB document by injecting it into a fresh LLM context (Groq Llama 3.3 70B)
and verifying it answers expected questions correctly.

Run from the PROJECT ROOT (c:\\...\\Oracle-Forge-data-agent\\):

    python kb/injection_test.py --kb-path ./kb
    python kb/injection_test.py --kb-path ./kb --verbose
    python kb/injection_test.py --kb-path ./kb --update-log
    python kb/injection_test.py --kb-path ./kb --test-single architecture/memory.md
    python kb/injection_test.py --kb-path ./kb --validate-paths

Or use the convenience script at the project root:

    python run_injection_tests.py
    python run_injection_tests.py --verbose
    python run_injection_tests.py --validate-paths
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from groq import Groq

# ============================================================================
# CONFIGURATION
# ============================================================================

# Expected answers for each KB document (from injection tests)
# Format: {document_path: {"question": str, "expected_answer_contains": list, "expected_exact": str}}
EXPECTED_ANSWERS = {
    "architecture/memory.md": {
        "question": "What are the three layers of Claude Code's memory system?",
        "expected_contains": ["MEMORY.md", "topic files", "session transcripts"],
        "expected_exact": None
    },
    "architecture/autodream_consolidation.md": {
        "question": "When does autoDream run and what does it do?",
        "expected_contains": ["Fridays", "compresses", "session transcripts", "resolved_patterns"],
        "expected_exact": None
    },
    "architecture/tool_scoping_philosophy.md": {
        "question": "Why are 40+ tight tools better than 5 generic tools?",
        "expected_contains": ["narrow", "precise", "DB-specific", "boundaries"],
        "expected_exact": None
    },
    "architecture/openai_layers.md": {
        "question": "What are the minimum three context layers for Oracle Forge?",
        "expected_contains": ["Schema", "institutional", "correction log"],
        "expected_exact": None
    },
    "architecture/conductor_worker_pattern.md": {
        "question": "How does the agent handle multi-database queries?",
        "expected_contains": ["Conductor", "spawns", "workers", "merges"],
        "expected_exact": None
    },
    "architecture/evaluation_harness_schema.md": {
        "question": "What is pass@1 and how is it calculated?",
        "expected_contains": ["correct first answers", "queries", "minimum"],
        "expected_exact": None
    },
    "domain/databases/postgresql_schemas.md": {
        "question": "What is the format of Yelp business_id?",
        "expected_contains": ["abc123def456"],
        "expected_exact": None
    },
    "domain/databases/mongodb_schemas.md": {
        "question": "What is the format of customer_id in MongoDB telecom collection?",
        "expected_contains": ["CUST-", "STRING", "prefix"],
        "expected_exact": None
    },
    "domain/databases/sqlite_schemas.md": {
        "question": "What format are customer_ids in SQLite?",
        "expected_contains": ["INTEGER", "no prefix"],
        "expected_exact": None
    },
    "domain/databases/duckdb_schemas.md": {
        "question": "What is DuckDB used for in DAB?",
        "expected_contains": ["analytical", "aggregate", "large datasets"],
        "expected_exact": None
    },
    "domain/joins/join_key_mappings.md": {
        "question": "How do I join PostgreSQL subscriber_id to MongoDB?",
        "expected_contains": ["resolve_join_key", "CUST-", "transformation"],
        "expected_exact": None
    },
    "domain/joins/cross_db_join_patterns.md": {
        "question": "What are the steps for PostgreSQL to MongoDB join?",
        "expected_contains": ["Query PG", "transform", "query Mongo", "merge"],
        "expected_exact": None
    },
    "domain/unstructured/text_extraction_patterns.md": {
        "question": "How do I extract negative sentiment from support ticket text?",
        "expected_contains": ["negative_indicators", ".lower()", "any()"],
        "expected_exact": None
    },
    "domain/unstructured/sentiment_mapping.md": {
        "question": "How does negation affect sentiment classification?",
        "expected_contains": ["not good", "negative", "not bad", "non-negative"],
        "expected_exact": None
    },
    "domain/domain_terms/business_glossary.md": {
        "question": "What does 'active customer' mean in telecom?",
        "expected_contains": ["last 90 days", "churn_date IS NULL"],
        "expected_exact": None
    },
    "corrections/failure_log.md": {
        "question": "What went wrong on Q023 and what's the fix?",
        "expected_contains": ["INT", "resolve_join_key"],
        "expected_exact": None
    },
    "corrections/failure_by_category.md": {
        "question": "What are DAB's four failure categories?",
        "expected_contains": ["Multi-Database", "Join Key", "Unstructured", "Domain Knowledge"],
        "expected_exact": None
    },
    "corrections/resolved_patterns.md": {
        "question": "What is the confidence score for PG-INT to Mongo-String transformation?",
        "expected_contains": ["14/14", "successes"],
        "expected_exact": None
    },
    "corrections/regression_prevention.md": {
        "question": "What happens if regression test fails?",
        "expected_contains": ["Revert", "log failure", "do not deploy"],
        "expected_exact": None
    },
    "evaluation/dab_scoring_method.md": {
        "question": "What is pass@1?",
        "expected_contains": ["correct first answers", "total queries"],
        "expected_exact": None
    },
    "evaluation/submission_format.md": {
        "question": "What files are required for DAB submission?",
        "expected_contains": ["results JSON", "AGENT.md"],
        "expected_exact": None
    }
}


# ============================================================================
# LLM CLIENT
# ============================================================================

class GroqLLMClient:
    """Client for Groq's Llama 3.3 70B model"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "llama-3.1-8b-instant"):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment or arguments")
        
        self.client = Groq(api_key=self.api_key)
        self.model = model
    
    def query(self, system_prompt: str, user_question: str, temperature: float = 0.0,
              max_retries: int = 3) -> str:
        """
        Send a query to the LLM with ONLY the document as context.
        Temperature=0.0 for deterministic, reproducible answers.

        Raises on API errors so callers can distinguish errors from failures:
        - TPD (tokens per day) exhausted → raises immediately (no retry useful)
        - Transient 429 (tokens per minute) → retries up to max_retries with backoff
        - Other exceptions → re-raised immediately
        """
        last_exc: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_question}
                    ],
                    temperature=temperature,
                    max_tokens=500
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                error_str = str(e)
                last_exc = e
                # Daily token cap exhausted — retrying won't help, surface immediately
                if "tokens per day" in error_str or "TPD" in error_str:
                    raise
                # Transient rate limit (tokens per minute) — backoff and retry
                if "429" in error_str or "rate_limit" in error_str:
                    if attempt < max_retries - 1:
                        wait = 15 * (2 ** attempt)  # 15s → 30s → 60s
                        print(f"  ⏳ Rate limited (attempt {attempt + 1}/{max_retries}),"
                              f" retrying in {wait}s...")
                        time.sleep(wait)
                        continue
                # Any other error — don't retry
                raise
        raise last_exc  # type: ignore[misc]


# ============================================================================
# INJECTION TESTER
# ============================================================================

class InjectionTester:
    """Runs injection tests on KB documents"""
    
    def __init__(self, kb_path: Path, llm_client: GroqLLMClient, verbose: bool = False,
                 delay: float = 1.0):
        self.kb_path = kb_path
        self.llm = llm_client
        self.verbose = verbose
        self.delay = delay  # seconds to sleep between API calls
        self.results = []
    
    def read_document(self, rel_path: str) -> Optional[str]:
        """Read a KB document from the filesystem"""
        full_path = self.kb_path / rel_path
        if not full_path.exists():
            return None
        with open(full_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def test_document(self, rel_path: str, expected: Dict) -> Dict:
        """
        Test a single document by injecting it into LLM context
        and verifying the answer contains expected content.
        """
        result = {
            "document": rel_path,
            "passed": False,
            "llm_answer": None,
            "error": None,
            "matched_keywords": [],
            "missing_keywords": []
        }
        
        # Read document content
        content = self.read_document(rel_path)
        if content is None:
            result["error"] = f"Document not found: {rel_path}"
            return result
        
        # Build system prompt - ONLY the document content
        system_prompt = f"""You are a test harness. You have been given EXACTLY ONE document as your only source of knowledge.
You must answer questions using ONLY the information in this document.
If the document does not contain the answer, say "I cannot answer from the provided document."
Do not use any prior knowledge.

Here is the document:

{content}

Remember: Answer ONLY from the document above."""

        # Get LLM answer
        try:
            answer = self.llm.query(system_prompt, expected["question"])
            result["llm_answer"] = answer

            if self.verbose:
                print(f"\n  Question: {expected['question']}")
                print(f"  Answer: {answer[:200]}...")

            # Verify answer contains expected keywords
            if expected.get("expected_contains"):
                keywords = expected["expected_contains"]
                for keyword in keywords:
                    if keyword.lower() in answer.lower():
                        result["matched_keywords"].append(keyword)
                    else:
                        result["missing_keywords"].append(keyword)

                # Pass if at least 70% of keywords match
                match_rate = len(result["matched_keywords"]) / len(keywords)
                result["passed"] = match_rate >= 0.7
                result["match_rate"] = match_rate

            elif expected.get("expected_exact"):
                result["passed"] = answer.strip().lower() == expected["expected_exact"].lower()
                result["match_rate"] = 1.0 if result["passed"] else 0.0

        except Exception as e:
            # Store in error field (not llm_answer) so summarize() counts it as
            # an error — not a failure — and the pass rate is not polluted.
            result["error"] = str(e)
            result["llm_answer"] = None
            result["match_rate"] = None
        
        return result
    
    def validate_paths(self) -> dict:
        """
        Scan all KB documents for internal kb/ path references and verify
        each referenced path actually exists on disk.
        Catches stale paths that injection tests (content-only) cannot detect.
        """
        import re
        path_pattern = re.compile(r'kb/([\w/.-]+\.(?:md|py|json|sh))')

        print(f"\n{'='*60}")
        print("KB PATH VALIDATION")
        print(f"{'='*60}")

        broken: list = []
        checked = 0

        for rel_path in EXPECTED_ANSWERS.keys():
            content = self.read_document(rel_path)
            if content is None:
                continue
            for match in path_pattern.findall(content):
                checked += 1
                if not (self.kb_path / match).exists():
                    broken.append({"document": rel_path, "broken_path": f"kb/{match}"})
                    print(f"  ❌ BROKEN: {rel_path} → kb/{match}")

        if not broken:
            print(f"  ✅ All {checked} path references are valid")
        else:
            print(f"\n  {len(broken)} broken path(s) found — fix before running agent")

        print(f"{'='*60}")
        return {"checked": checked, "broken": broken, "valid": len(broken) == 0}

    def run_all_tests(self) -> Dict:
        """Run injection tests for all documents in EXPECTED_ANSWERS"""
        print(f"\n{'='*60}")
        print(f"KB INJECTION TEST SUITE")
        print(f"{'='*60}")
        print(f"Model: {self.llm.model}")
        print(f"KB Path: {self.kb_path}")
        print(f"Documents to test: {len(EXPECTED_ANSWERS)}")
        print(f"{'='*60}\n")
        
        total_docs = len(EXPECTED_ANSWERS)
        for idx, (rel_path, expected) in enumerate(EXPECTED_ANSWERS.items()):
            print(f"Testing [{idx + 1}/{total_docs}]: {rel_path}")
            result = self.test_document(rel_path, expected)
            self.results.append(result)

            # Print result
            if result["passed"]:
                print(f"  ✅ PASSED (match rate: {result.get('match_rate', 1.0)*100:.0f}%)")
            elif result["error"]:
                # Truncate long API error messages for readability
                err_preview = result["error"][:120].replace("\n", " ")
                print(f"  ⚠️  ERROR: {err_preview}")
            else:
                print(f"  ❌ FAILED (matched: {result['matched_keywords']})")
                print(f"     Missing: {result['missing_keywords']}")

            if self.verbose and result.get("llm_answer"):
                print(f"  Answer excerpt: {result['llm_answer'][:150]}...")

            # Pace requests to avoid hitting daily token cap
            if self.delay > 0 and idx < total_docs - 1:
                time.sleep(self.delay)
        
        # Summary
        return self.summarize()
    
    def test_single_document(self, rel_path: str) -> Dict:
        """Test a single document by path"""
        if rel_path not in EXPECTED_ANSWERS:
            print(f"Warning: {rel_path} not in EXPECTED_ANSWERS. Using generic test.")
            return {"error": "No test definition found"}
        
        result = self.test_document(rel_path, EXPECTED_ANSWERS[rel_path])
        self.results = [result]
        return self.summarize()
    
    def summarize(self) -> Dict:
        """Generate test summary"""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.get("passed", False))
        failed = sum(1 for r in self.results if not r.get("passed", False) and not r.get("error"))
        errors = sum(1 for r in self.results if r.get("error"))
        
        summary = {
            "total": total,
            "passed": passed,
            "failed": failed,
            "errors": errors,
            "pass_rate": passed / total if total > 0 else 0,
            "results": self.results
        }
        
        print(f"\n{'='*60}")
        print(f"TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Total Documents: {total}")
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"⚠️  Errors: {errors}")
        print(f"Pass Rate: {summary['pass_rate']*100:.1f}%")
        print(f"{'='*60}")
        
        if failed > 0:
            print("\nFailed Documents:")
            for r in self.results:
                if not r.get("passed") and not r.get("error"):
                    print(f"  - {r['document']}")
                    print(f"    Missing: {r.get('missing_keywords', [])}")
        
        if errors > 0:
            print("\nErrors:")
            for r in self.results:
                if r.get("error"):
                    print(f"  - {r['document']}: {r['error']}")
        
        return summary
    
    def save_results(self, output_path: Path):
        """Save full test results to JSON file."""
        summary = self.summarize()
        summary["timestamp"] = datetime.now().isoformat()
        summary["model"] = self.llm.model

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"Results saved to: {output_path}")

    def save_markdown_report(self, output_path: Path):
        """
        Save full test results as a Markdown report mirroring the JSON structure.
        One section per document: question, LLM answer, matched/missing keywords, match rate.
        """
        summary = self.summarize()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        passed = summary["passed"]
        total = summary["total"]
        failed = summary["failed"]
        errors = summary["errors"]
        pass_rate = summary["pass_rate"] * 100

        lines = []
        lines.append("# KB Injection Test Report\n")
        lines.append(f"**Date:** {timestamp}  ")
        lines.append(f"**Model:** {self.llm.model}  ")
        lines.append(f"**KB Path:** {self.kb_path}  ")
        lines.append(f"**Pass Rate:** {passed}/{total} ({pass_rate:.1f}%)\n")

        # ── Summary table ──────────────────────────────────────────────
        lines.append("## Summary\n")
        lines.append("| Total | Passed | Failed | Errors | Pass Rate |")
        lines.append("|-------|--------|--------|--------|-----------|")
        lines.append(f"| {total} | {passed} | {failed} | {errors} | {pass_rate:.1f}% |\n")

        # ── Failed list (if any) ───────────────────────────────────────
        if failed > 0:
            lines.append("## Failed Documents\n")
            for r in self.results:
                if not r.get("passed") and not r.get("error"):
                    missing = ", ".join(r.get("missing_keywords", []))
                    lines.append(f"- **{r['document']}** — missing keywords: `{missing}`")
            lines.append("")

        # ── Per-document results ───────────────────────────────────────
        lines.append("## Results by Document\n")

        for r in self.results:
            status = "PASS" if r.get("passed") else ("ERROR" if r.get("error") else "FAIL")
            icon = "✅" if status == "PASS" else ("⚠️" if status == "ERROR" else "❌")
            match_pct = f"{r.get('match_rate', 0) * 100:.0f}%" if r.get("match_rate") is not None else "—"

            lines.append(f"### {icon} {r['document']} — {status} ({match_pct})\n")

            # Question from EXPECTED_ANSWERS
            expected = EXPECTED_ANSWERS.get(r["document"], {})
            if expected.get("question"):
                lines.append(f"**Question:** {expected['question']}  ")

            # LLM answer
            if r.get("llm_answer"):
                answer = r["llm_answer"].replace("\n", " ").strip()
                lines.append(f"**LLM Answer:** {answer}  ")

            # Keyword match table
            matched = r.get("matched_keywords", [])
            missing = r.get("missing_keywords", [])
            matched_str = ", ".join(f"`{k}`" for k in matched) if matched else "—"
            missing_str = ", ".join(f"`{k}`" for k in missing) if missing else "—"
            lines.append(f"**Matched:** {matched_str}  ")
            lines.append(f"**Missing:** {missing_str}  ")
            lines.append(f"**Match Rate:** {match_pct}  ")

            if r.get("error"):
                lines.append(f"**Error:** {r['error']}  ")

            lines.append("\n---\n")

        report = "\n".join(lines)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(report)

        print(f"Markdown report saved to: {output_path}")

    def update_injection_test_log(self, log_path: Path):
        """
        Overwrite INJECTION_TEST_LOG.md with the full per-document report
        for the current run, preserving a compact historical run table at the top.
        """
        timestamp_short = datetime.now().strftime("%Y-%m-%d")
        timestamp_full = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        summary = self.summarize()
        passed = summary["passed"]
        total = summary["total"]
        pass_rate = summary["pass_rate"] * 100
        result_label = "PASS" if summary["failed"] == 0 and summary["errors"] == 0 else "FAIL"

        # ── Read or initialise historical run table ────────────────────
        history_rows: list = []
        if log_path.exists():
            with open(log_path, 'r', encoding='utf-8') as f:
                existing = f.read()
            # Extract existing history rows (lines starting with "| 20")
            for line in existing.splitlines():
                if line.startswith("| 20") and "Full Suite" in line:
                    # Skip today's row — we'll re-add it updated
                    if not line.startswith(f"| {timestamp_short}"):
                        history_rows.append(line)

        # Prepend today's row (most recent first)
        today_row = (
            f"| {timestamp_short} | Full Suite | {passed}/{total} | "
            f"{pass_rate:.1f}% | {result_label} |"
        )
        history_rows.insert(0, today_row)

        # ── Build full report ──────────────────────────────────────────
        lines = []
        lines.append("# KB Injection Test Log\n")
        lines.append("## Test Protocol\n")
        lines.append("1. Fresh LLM session with ONLY the document as context")
        lines.append("2. Ask the document's own Q&A question")
        lines.append("3. PASS = answer contains ≥70% expected keywords\n")

        lines.append("## Run History\n")
        lines.append("| Date | Scope | Score | Pass Rate | Result |")
        lines.append("|------|-------|-------|-----------|--------|")
        for row in history_rows:
            lines.append(row)
        lines.append("")

        lines.append(f"## Latest Run — {timestamp_full}\n")
        lines.append(f"**Model:** {self.llm.model}  ")
        lines.append(f"**Pass Rate:** {passed}/{total} ({pass_rate:.1f}%)  ")
        lines.append(f"**Path Validation:** all internal `kb/` references valid\n")

        # Summary table
        lines.append("| Total | Passed | Failed | Errors |")
        lines.append("|-------|--------|--------|--------|")
        lines.append(
            f"| {total} | {passed} | {summary['failed']} | {summary['errors']} |\n"
        )

        # Failed list
        if summary["failed"] > 0:
            lines.append("### Failed Documents\n")
            for r in self.results:
                if not r.get("passed") and not r.get("error"):
                    missing = ", ".join(r.get("missing_keywords", []))
                    lines.append(f"- **{r['document']}** — missing: `{missing}`")
            lines.append("")

        # Per-document detail
        lines.append("## Per-Document Results\n")
        for r in self.results:
            status = "PASS" if r.get("passed") else ("ERROR" if r.get("error") else "FAIL")
            icon = "✅" if status == "PASS" else ("⚠️" if status == "ERROR" else "❌")
            match_pct = f"{r.get('match_rate', 0) * 100:.0f}%" if r.get("match_rate") is not None else "—"

            lines.append(f"### {icon} {r['document']} — {match_pct}\n")

            expected = EXPECTED_ANSWERS.get(r["document"], {})
            if expected.get("question"):
                lines.append(f"**Q:** {expected['question']}  ")

            if r.get("llm_answer"):
                answer = r["llm_answer"].replace("\n", " ").strip()
                lines.append(f"**A:** {answer}  ")

            matched = r.get("matched_keywords", [])
            missing_kw = r.get("missing_keywords", [])
            lines.append(
                f"**Matched:** {', '.join(f'`{k}`' for k in matched) or '—'}  "
            )
            if missing_kw:
                lines.append(
                    f"**Missing:** {', '.join(f'`{k}`' for k in missing_kw)}  "
                )
            lines.append(f"**Match Rate:** {match_pct}\n")

        content = "\n".join(lines)
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(content)

        print(f"Injection test log updated: {log_path}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Injection test for KB documents using Groq Llama")
    parser.add_argument("--kb-path", type=str, default="./kb", help="Path to KB directory")
    parser.add_argument("--model", type=str, default="llama-3.1-8b-instant", help="Groq model name")
    parser.add_argument("--api-key", type=str, help="Groq API key (or set GROQ_API_KEY env var)")
    parser.add_argument("--test-single", type=str, help="Test a single document (relative path from kb)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output JSON file path. Defaults to injection_results/injection_test_YYYY-MM-DD_HH-MM-SS.json"
    )
    parser.add_argument(
        "--results-dir", type=str, default="./injection_results",
        help="Folder where JSON and MD results are saved (default: ./injection_results)"
    )
    parser.add_argument("--update-log", action="store_true", help="Update INJECTION_TEST_LOG.md")
    parser.add_argument("--validate-paths", action="store_true", help="Scan KB docs for broken internal paths (no LLM call)")
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds to sleep between API calls (default: 1.0). Increase to avoid TPD rate limits."
    )
    
    args = parser.parse_args()
    
    # Initialize tester (path validation needs no LLM)
    kb_path = Path(args.kb_path)
    if not kb_path.exists():
        print(f"❌ KB path not found: {kb_path}")
        sys.exit(1)

    # Path-only validation — no LLM required
    if args.validate_paths:
        # Use a dummy LLM client stub just to construct the tester
        class _Stub:
            model = "none"
        tester = InjectionTester(kb_path, _Stub(), verbose=False)  # type: ignore[arg-type]
        path_result = tester.validate_paths()
        sys.exit(0 if path_result["valid"] else 1)

    # Initialize LLM client
    try:
        llm = GroqLLMClient(api_key=args.api_key, model=args.model)
        print(f"✅ Connected to Groq API with model: {args.model}")
    except ValueError as e:
        print(f"❌ {e}")
        print("Set GROQ_API_KEY environment variable or use --api-key")
        sys.exit(1)

    tester = InjectionTester(kb_path, llm, verbose=args.verbose, delay=args.delay)

    # Always run path validation before LLM tests
    path_result = tester.validate_paths()
    if not path_result["valid"]:
        print("\n⚠️  Fix broken paths before running injection tests.")
        sys.exit(1)

    # Run tests
    if args.test_single:
        result = tester.test_single_document(args.test_single)
    else:
        result = tester.run_all_tests()
    
    # Resolve output directory and auto-generate timestamped filenames
    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        timestamp_file = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = results_dir / f"injection_test_{timestamp_file}.json"

    md_output_path = output_path.with_suffix(".md")

    # Save results — always write both JSON and matching .md report
    tester.save_results(output_path)
    tester.save_markdown_report(md_output_path)

    print(f"\nOutput folder : {results_dir.resolve()}")
    print(f"  JSON        : {output_path.name}")
    print(f"  Markdown    : {md_output_path.name}")

    # Update the KB log with full per-document detail
    if args.update_log:
        log_path = kb_path / "INJECTION_TEST_LOG.md"
        tester.update_injection_test_log(log_path)
    
    # Exit with appropriate code
    if result.get("failed", 0) > 0 or result.get("errors", 0) > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()