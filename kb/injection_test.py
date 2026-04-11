#!/usr/bin/env python3
"""
Injection Test Script for Knowledge Base Documents
Tests each KB document by injecting it into a fresh LLM context (Groq Llama 3.3 70B)
and verifying it answers expected questions correctly.

Usage:
    python injection_test.py --kb-path ./kb --model llama-3.3-70b-versatile
    python injection_test.py --kb-path ./kb --test-single v1-architecture/01_three_layer_memory.md
    python injection_test.py --kb-path ./kb --verbose
"""

import os
import sys
import json
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
        "expected_contains": ["correct first answers", "total queries", "50 trials"],
        "expected_exact": None
    },
    "domain/databases/postgresql_schemas.md": {
        "question": "What is the format of Yelp business_id?",
        "expected_contains": ["abc123def456", "TEXT"],
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
    "correction/failure_log.md": {
        "question": "What went wrong on Q023 and what's the fix?",
        "expected_contains": ["INT to String", "resolve_join_key"],
        "expected_exact": None
    },
    "correction/failure_by_category.md": {
        "question": "What are DAB's four failure categories?",
        "expected_contains": ["Multi-Database", "Join Key", "Unstructured", "Domain Knowledge"],
        "expected_exact": None
    },
    "correction/resolved_patterns.md": {
        "question": "What is the confidence score for PG-INT to Mongo-String transformation?",
        "expected_contains": ["14/14", "successes"],
        "expected_exact": None
    },
    "correction/regression_prevention.md": {
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
    
    def __init__(self, api_key: Optional[str] = None, model: str = "llama-3.3-70b-versatile"):
        self.api_key = api_key or os.environ.get("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("GROQ_API_KEY not found in environment or arguments")
        
        self.client = Groq(api_key=self.api_key)
        self.model = model
    
    def query(self, system_prompt: str, user_question: str, temperature: float = 0.0) -> str:
        """
        Send a query to the LLM with ONLY the document as context.
        Temperature=0.0 for deterministic, reproducible answers.
        """
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
            return f"ERROR: {str(e)}"


# ============================================================================
# INJECTION TESTER
# ============================================================================

class InjectionTester:
    """Runs injection tests on KB documents"""
    
    def __init__(self, kb_path: Path, llm_client: GroqLLMClient, verbose: bool = False):
        self.kb_path = kb_path
        self.llm = llm_client
        self.verbose = verbose
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
            result["error"] = str(e)
        
        return result
    
    def run_all_tests(self) -> Dict:
        """Run injection tests for all documents in EXPECTED_ANSWERS"""
        print(f"\n{'='*60}")
        print(f"KB INJECTION TEST SUITE")
        print(f"{'='*60}")
        print(f"Model: {self.llm.model}")
        print(f"KB Path: {self.kb_path}")
        print(f"Documents to test: {len(EXPECTED_ANSWERS)}")
        print(f"{'='*60}\n")
        
        for rel_path, expected in EXPECTED_ANSWERS.items():
            print(f"Testing: {rel_path}")
            result = self.test_document(rel_path, expected)
            self.results.append(result)
            
            # Print result
            if result["passed"]:
                print(f"  ✅ PASSED (match rate: {result.get('match_rate', 1.0)*100:.0f}%)")
            elif result["error"]:
                print(f"  ❌ ERROR: {result['error']}")
            else:
                print(f"  ❌ FAILED (matched: {result['matched_keywords']})")
                print(f"     Missing: {result['missing_keywords']}")
            
            if self.verbose and result.get("llm_answer"):
                print(f"  Answer excerpt: {result['llm_answer'][:150]}...")
        
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
        """Save test results to JSON file"""
        summary = self.summarize()
        summary["timestamp"] = datetime.now().isoformat()
        summary["model"] = self.llm.model
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"\nResults saved to: {output_path}")
    
    def update_injection_test_log(self, log_path: Path):
        """Update INJECTION_TEST_LOG.md with new results"""
        timestamp = datetime.now().strftime("%Y-%m-%d")
        summary = self.summarize()
        
        log_entry = f"\n| {timestamp} | Full Suite | All documents | {summary['passed']}/{summary['total']} passed | {summary['pass_rate']*100:.0f}% |\n"
        
        # Check if log exists
        if log_path.exists():
            with open(log_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Check if entry already exists for today
            if timestamp in content:
                # Replace existing entry
                lines = content.split('\n')
                new_lines = []
                skip_next = False
                for line in lines:
                    if line.startswith(f"| {timestamp}"):
                        skip_next = True
                        continue
                    if skip_next and line.startswith('|'):
                        skip_next = False
                    new_lines.append(line)
                content = '\n'.join(new_lines)
            
            # Find the table body and insert
            lines = content.split('\n')
            new_content = []
            inserted = False
            for line in lines:
                new_content.append(line)
                if '| Date | Document | Test Question | Expected | Result |' in line and not inserted:
                    new_content.append('|------|----------|---------------|----------|--------|')
                    new_content.append(log_entry.strip())
                    inserted = True
            
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(new_content))
        else:
            # Create new log
            log_content = f"""# KB Document Injection Test Log

## Test Protocol
1. Start fresh LLM session with ONLY document as context
2. Ask question the document should answer
3. PASS = correct answer, FAIL = revise document

## Test Results

| Date | Document | Test Question | Expected | Result |
|------|----------|---------------|----------|--------|
{log_entry}

## Summary
- Last Test Run: {timestamp}
- Pass Rate: {summary['pass_rate']*100:.0f}%
- Total Documents: {summary['total']}
"""
            with open(log_path, 'w', encoding='utf-8') as f:
                f.write(log_content)
        
        print(f"Injection test log updated: {log_path}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Injection test for KB documents using Groq Llama")
    parser.add_argument("--kb-path", type=str, default="./kb", help="Path to KB directory")
    parser.add_argument("--model", type=str, default="llama-3.3-70b-versatile", help="Groq model name")
    parser.add_argument("--api-key", type=str, help="Groq API key (or set GROQ_API_KEY env var)")
    parser.add_argument("--test-single", type=str, help="Test a single document (relative path from kb)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--output", type=str, default="./injection_test_results.json", help="Output JSON file")
    parser.add_argument("--update-log", action="store_true", help="Update INJECTION_TEST_LOG.md")
    
    args = parser.parse_args()
    
    # Initialize LLM client
    try:
        llm = GroqLLMClient(api_key=args.api_key, model=args.model)
        print(f"✅ Connected to Groq API with model: {args.model}")
    except ValueError as e:
        print(f"❌ {e}")
        print("Set GROQ_API_KEY environment variable or use --api-key")
        sys.exit(1)
    
    # Initialize tester
    kb_path = Path(args.kb_path)
    if not kb_path.exists():
        print(f"❌ KB path not found: {kb_path}")
        sys.exit(1)
    
    tester = InjectionTester(kb_path, llm, verbose=args.verbose)
    
    # Run tests
    if args.test_single:
        result = tester.test_single_document(args.test_single)
    else:
        result = tester.run_all_tests()
    
    # Save results
    output_path = Path(args.output)
    tester.save_results(output_path)
    
    # Update injection test log
    if args.update_log:
        log_path = kb_path / "INJECTION_TEST_LOG.md"
        tester.update_injection_test_log(log_path)
    
    # Exit with appropriate code
    if result.get("failed", 0) > 0 or result.get("errors", 0) > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()