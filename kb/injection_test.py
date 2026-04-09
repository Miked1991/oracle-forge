#!/usr/bin/env python3
"""
KB Injection Test Framework - Groq Llama Version

Tests that each KB document can be injected into a fresh LLM context
and produce correct answers without additional explanation.

Uses Groq's Llama 3 model (fast and free tier available).

Usage:
    python3 kb/injection_test_groq.py   # Run all tests
    python3 kb/injection_test_groq.py --document kb/domain/join_keys.md
    python3 kb/injection_test_groq.py --model llama3-70b-8192
    python3 kb/injection_test_groq.py --verbose
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class GroqLlamaLLM:
    """
    Groq Llama LLM wrapper for injection tests.
    
    Available models:
    - "llama-3.1-8b-instant"
    - "llama-3.3-70b-versatile"
    """
    
    def __init__(self, model: str = "llama-3.3-70b-versatile", temperature: float = 0.0):
        self.model = model
        self.temperature = temperature
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of Groq client"""
        if self._client is None:
            try:
                from groq import Groq
                api_key = os.environ.get("GROQ_API_KEY")
                if not api_key:
                    raise ValueError("GROQ_API_KEY environment variable not set")
                self._client = Groq(api_key=api_key)
            except ImportError:
                raise ImportError("Please install groq: pip install groq")
        return self._client
    
    def generate(self, prompt: str) -> str:
        """Generate response from Groq Llama model"""
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precise answer engine. Answer based ONLY on the provided document. Be specific and concise. Do not add information not in the document."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=self.temperature,
                max_tokens=500,
                top_p=1.0
            )
            return completion.choices[0].message.content
        except Exception as e:
            return f"ERROR: {str(e)}"


class MockLLM:
    """Mock LLM for testing without API key"""
    
    def generate(self, prompt: str) -> str:
        prompt_lower = prompt.lower()
        
        if "customerid" in prompt_lower and "postgresql" in prompt_lower:
            return "CustomerID in PostgreSQL is an integer. In MongoDB it is stored as a string with CUST- prefix."
        elif "active customer" in prompt_lower:
            return "An active customer is one who has purchased in the last 90 days."
        elif "churn" in prompt_lower:
            return "Churn is when a customer who had activity in the previous period has no activity in the current period."
        elif "tables" in prompt_lower and "yelp" in prompt_lower:
            return "The Yelp PostgreSQL database has business, review, and user tables."
        elif "cast" in prompt_lower:
            return "Use CAST(customer_id AS TEXT) to convert integer to string."
        elif "sentiment" in prompt_lower:
            return "Extract sentiment using sandbox with keyword matching before counting."
        elif "fiscal" in prompt_lower:
            return "Fiscal year starts on July 1."
        elif "gmv" in prompt_lower:
            return "GMV means Gross Merchandise Value, which is total sales before returns."
        else:
            return "Based on the document, the answer is provided in the text above."


class InjectionTestRunner:
    """
    Runs injection tests on KB documents using Groq Llama.
    
    A document PASSES if a fresh LLM with ONLY that document
    can correctly answer a predefined test question.
    """
    
    def __init__(self, verbose: bool = False, use_mock: bool = False, 
                 model: str = "llama-3.3-70b-versatile"):
        self.verbose = verbose
        self.use_mock = use_mock
        self.model = model
        self.results = []
        
        # Build test suite
        self.test_suite = self._build_test_suite()
    
    def _build_test_suite(self) -> Dict[str, List[Tuple[str, str]]]:
        """
        Build test suite mapping documents to test questions.
        
        Each entry: (document_path, test_question, expected_answer_contains)
        """
        return {
            # Architecture documents
            "kb/architecture/schemas.md": [
                (
                    "What tables are in the Yelp PostgreSQL database?",
                    "yelp_business,yelp_review,yelp_user"
                ),
                (
                    "What columns does the yelp_review table have?",
                    "review_id,user_id,business_id,stars,text,date,useful,funny,cool"
                ),
                (
                    "What is the data type of stars in the review table?",
                    "integer"
                ),
            ],
            
            "kb/architecture/claude_code_memory.md": [
                (
                    "What are the three layers of Claude Code's memory system?",
                    "MEMORY.md"
                ),
                (
                    "What is the autoDream consolidation pattern?",
                    "consolidat"
                ),
            ],
            
            "kb/architecture/openai_six_layers.md": [
                (
                    "How many context layers does OpenAI's data agent use?",
                    "6"
                ),
                (
                    "What is the hardest sub-problem mentioned?",
                    "table"
                ),
            ],
            
            # Domain documents
            "kb/domain/join_keys.md": [
                (
                    "How is CustomerID formatted in Yelp PostgreSQL versus MongoDB?",
                    "integer in PostgreSQL"
                ),
                (
                    "What transformation is needed to join PostgreSQL integer IDs with MongoDB CUST-prefixed IDs?",
                    "CAST"
                ),
                (
                    "How do you detect a prefix mismatch between databases?",
                    "prefix"
                ),
                (
                    "What is the resolution pattern for integer to string mismatch?",
                    "CAST"
                ),
            ],
            
            "kb/domain/terms.md": [
                (
                    "What does 'active customer' mean in the DAB datasets?",
                    "90 days"
                ),
                (
                    "When does the fiscal year start according to the KB?",
                    "July 1"
                ),
                (
                    "What does 'churn' mean?",
                    "previous period"
                ),
                (
                    "What does GMV stand for?",
                    "Gross Merchandise Value"
                ),
                (
                    "What is the formula for Repeat Purchase Rate?",
                    "Customers with"
                ),
            ],
            
            "kb/domain/query_patterns.md": [
                (
                    "How do you count records across multiple databases?",
                    "parallel"
                ),
                (
                    "What is the pattern for cross-database aggregation?",
                    "merge"
                ),
            ],
            
            # Evaluation documents
            "kb/evaluation/dab_format.md": [
                (
                    "What is the pass@1 metric?",
                    "first trial"
                ),
                (
                    "How many trials are recommended per query?",
                    "5"
                ),
            ],
            
            # Corrections log
            "kb/corrections/corrections.md": [
                (
                    "What was the fix for the join key mismatch failure?",
                    "CAST"
                ),
                (
                    "How should sentiment extraction be done?",
                    "sandbox"
                ),
                (
                    "What was the issue with the active customer definition?",
                    "90 days"
                ),
            ],
        }
    
    def _read_document(self, doc_path: str) -> Optional[str]:
        """Read document content from file"""
        full_path = Path(doc_path)
        if not full_path.exists():
            return None
        with open(full_path, 'r') as f:
            return f.read()
    
    def test_document(self, doc_path: str, question: str, expected: str) -> Dict:
        """
        Test a single document-question pair.
        
        Returns:
            Dict with keys: passed, answer, expected, error
        """
        # Read document
        doc_content = self._read_document(doc_path)
        if doc_content is None:
            return {
                "passed": False,
                "answer": None,
                "expected": expected,
                "error": f"Document not found: {doc_path}"
            }
        
        # Build prompt - ONLY the document, no extra instructions
        prompt = f"""Document:
{doc_content}

Question: {question}

Answer based ONLY on the document above. Be specific and concise. If the answer is not in the document, say "Information not found in document"."""

        # Call LLM
        if self.use_mock:
            llm = MockLLM()
        else:
            llm = GroqLlamaLLM(model=self.model)
        
        try:
            answer = llm.generate(prompt)
        except Exception as e:
            return {
                "passed": False,
                "answer": None,
                "expected": expected,
                "error": str(e)
            }
        
        # Check if answer contains all expected tokens (case-insensitive).
        # Expected may be comma-separated keywords; all must appear in the answer.
        answer_lower = answer.lower()
        tokens = [t.strip() for t in expected.lower().split(",") if t.strip()]
        passed = all(token in answer_lower for token in tokens)
        
        return {
            "passed": passed,
            "answer": answer,
            "expected": expected,
            "error": None,
            "question": question,
            "doc_path": doc_path
        }
    
    def run_all(self) -> Dict:
        """Run all tests in the test suite"""
        print("\n" + "=" * 70)
        print("KB INJECTION TEST SUITE - Groq Llama")
        print("=" * 70)
        print(f"Time: {datetime.now().isoformat()}")
        print(f"Model: {self.model if not self.use_mock else 'MOCK MODE'}")
        print(f"Mode: {'MOCK' if self.use_mock else 'GROQ LLAMA'}")
        print("-" * 70)
        
        total_passed = 0
        total_failed = 0
        failed_tests = []
        
        for doc_path, test_cases in self.test_suite.items():
            # Check if document exists
            if not Path(doc_path).exists():
                print(f"\n⚠️ SKIPPING: {doc_path} (file not found)")
                continue
            
            print(f"\n📄 Testing: {doc_path}")
            
            for question, expected in test_cases:
                # Truncate question for display
                display_q = question[:50] + "..." if len(question) > 50 else question
                print(f"   ❓ {display_q}")
                
                result = self.test_document(doc_path, question, expected)
                
                if result["passed"]:
                    print(f"      ✅ PASS (found '{expected}')")
                    total_passed += 1
                else:
                    print(f"      ❌ FAIL (expected '{expected}')")
                    if self.verbose and result["answer"]:
                        print(f"         Got: {result['answer'][:150]}...")
                    elif not self.verbose:
                        print(f"         (Run with --verbose to see full response)")
                    total_failed += 1
                    failed_tests.append(result)
        
        # Summary
        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        print(f"✅ Passed: {total_passed}")
        print(f"❌ Failed: {total_failed}")
        total_tests = total_passed + total_failed
        if total_tests > 0:
            print(f"📊 Success rate: {total_passed/total_tests*100:.1f}%")
        
        if failed_tests:
            print("\n❌ FAILED TESTS DETAILS:")
            for i, ft in enumerate(failed_tests[:5], 1):  # Show first 5
                print(f"\n   {i}. Document: {ft['doc_path']}")
                print(f"      Question: {ft['question'][:80]}")
                print(f"      Expected: '{ft['expected']}'")
                print(f"      Got: '{ft['answer'][:100] if ft['answer'] else 'None'}...'")
        
        # Save results
        self._save_results(total_passed, total_failed, failed_tests)
        
        return {
            "passed": total_passed,
            "failed": total_failed,
            "success_rate": total_passed/total_tests if total_tests > 0 else 0,
            "overall_pass": total_failed == 0,
            "failed_tests": failed_tests
        }
    
    def _save_results(self, passed: int, failed: int, failed_tests: List[Dict]):
        """Save test results to file"""
        results_path = Path("kb/injection_test_results.json")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "model": self.model if not self.use_mock else "MOCK",
            "passed": passed,
            "failed": failed,
            "success_rate": passed/(passed+failed) if passed+failed > 0 else 0,
            "failed_tests": [
                {
                    "doc_path": ft["doc_path"],
                    "question": ft["question"],
                    "expected": ft["expected"],
                    "answer_preview": ft["answer"][:300] if ft["answer"] else None
                }
                for ft in failed_tests
            ]
        }
        
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n📁 Results saved to: {results_path}")


def check_groq_api_key() -> bool:
    """Check if Groq API key is configured"""
    api_key = os.getenv("GROQ_API_KEY")
    if api_key:
        print("✅ Groq API key found")
        return True
    else:
        print("❌ GROQ_API_KEY not set")
        print("   Set it with: export GROQ_API_KEY='your-key'")
        print("   Or add to .env file")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run KB injection tests with Groq Llama")
    parser.add_argument("--document", "-d", type=str, help="Test only this document")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show full LLM responses")
    parser.add_argument("--mock", "-m", action="store_true", help="Use mock LLM (no API key needed)")
    parser.add_argument("--model", "-M", type=str, default="llama-3.3-70b-versatile",
                        choices=["llama-3.3-70b-versatile", "llama3-70b-8192", "llama3-8b-8192",
                                "llama-3.1-70b-versatile", "llama-3.1-8b-instant"],
                        help="Groq model to use")
    parser.add_argument("--check-key", action="store_true", help="Check if Groq API key is configured")
    
    args = parser.parse_args()
    
    if args.check_key:
        check_groq_api_key()
        return
    
    if not args.mock and not check_groq_api_key():
        print("\n⚠️ No API key found. Use --mock for testing without API.")
        response = input("Continue with mock mode? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)
        args.mock = True
    
    runner = InjectionTestRunner(
        verbose=args.verbose, 
        use_mock=args.mock,
        model=args.model
    )
    
    if args.document:
        # Test single document
        doc_path = args.document
        if doc_path in runner.test_suite:
            print(f"\n📄 Testing single document: {doc_path}")
            for question, expected in runner.test_suite[doc_path]:
                print(f"\n❓ {question}")
                result = runner.test_document(doc_path, question, expected)
                status = "✅ PASS" if result["passed"] else "❌ FAIL"
                print(f"{status} (expected '{expected}')")
                if not result["passed"] and args.verbose:
                    print(f"   Got: {result['answer']}")
        else:
            print(f"Document not in test suite: {doc_path}")
            print("Available documents:")
            for d in runner.test_suite.keys():
                print(f"  - {d}")
            sys.exit(1)
    else:
        # Run all tests
        results = runner.run_all()
        sys.exit(0 if results["overall_pass"] else 1)


if __name__ == "__main__":
    main()