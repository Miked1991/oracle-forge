# Oracle Forge — Data Agent

A context-injection knowledge base for an LLM-powered multi-database analytics agent, built for the [DAB benchmark](https://github.com/DABenchmark).

## How It Works

Every file in `kb/` is a self-contained document designed to be injected directly into an LLM context window. No RAG, no embeddings — documents are loaded by path and pasted as system context before query execution.

Documents are validated with **injection tests**: a fresh LLM session receives only the document, is asked a question it should answer, and must match ≥70% of expected keywords to pass.

## Repository Structure

```markdown
oracle-forge-data-agent/
├── kb/                              # The agent's knowledge base
│   ├── architecture/                # How the agent thinks
│   │   ├── memory.md                  # Three-layer memory system
│   │   ├── conductor_worker_pattern.md # Multi-database routing
│   │   ├── openai_layers.md           # Six-layer context architecture
│   │   ├── autodream_consolidation.md # Weekly session compression
│   │   ├── tool_scoping_philosophy.md # 40+ tight tools > 5 generic
│   │   └── evaluation_harness_schema.md # Trace schema + pass@1
│   ├── domain/                      # DAB dataset knowledge
│   │   ├── databases/               # PostgreSQL, MongoDB, SQLite, DuckDB schemas
│   │   ├── joins/                   # Cross-DB join key transformations
│   │   ├── unstructured/            # Sentiment + text extraction patterns
│   │   └── domain_terms/            # Business glossary (telecom, Yelp, healthcare)
│   ├── correction/                  # Self-learning correction loop
│   │   ├── failure_log.md           # Chronological failures + fixes
│   │   ├── failure_by_category.md   # Failures by DAB's 4 categories
│   │   ├── resolved_patterns.md     # Permanent fixes with confidence scores
│   │   └── regression_prevention.md # Regression test rules
│   ├── evaluation/                  # DAB benchmark reference
│   │   ├── dab_scoring_method.md    # pass@1 definition and calculation
│   │   └── submission_format.md     # PR requirements + AGENT.md template
│   ├── injection_test.py            # Injection test runner (Groq Llama)
│   └── CHANGELOG.md                 # Version history
│
├── planning/                        # Team planning documents
├── requirements.txt                 # Python dependencies
└── setup_groq_tests.sh              # API key setup + test quickstart
```

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure Groq API key (interactive)
bash setup_groq_tests.sh

# Or set manually
export GROQ_API_KEY="your-key-here"
```

## Running Injection Tests

```bash
# Run the full suite (21 documents)
python kb/injection_test.py --kb-path ./kb

# Test a single document
python kb/injection_test.py --kb-path ./kb --test-single architecture/memory.md --verbose

# Save results to JSON
python kb/injection_test.py --kb-path ./kb --output injection_test_results.json
```

Current pass rate: **21/21 (100%)** — see `injection_test_results.json`.

## Session Start — Document Load Order

Inject these files at the start of every agent session:

1. `architecture/memory.md`
2. `architecture/conductor_worker_pattern.md`
3. `architecture/openai_layers.md`
4. `correction/failure_log.md`
5. `correction/resolved_patterns.md`

Then load on demand:

- `domain/databases/<db>_schemas.md` for each database type in the query
- `domain/joins/join_key_mappings.md` for any cross-database join
- `domain/domain_terms/business_glossary.md` for telecom / Yelp / healthcare queries

## Adding a KB Document

1. Create the file in the appropriate `kb/` subdirectory
2. Add a test case to `EXPECTED_ANSWERS` in `kb/injection_test.py`
3. Run `python kb/injection_test.py --test-single <path> --verbose`
4. Revise until the test passes, then add a `CHANGELOG.md` entry

## Attribution

- Three-layer memory + autoDream — Claude Code architecture (March 2026)
- Six-layer context — OpenAI data agent writeup (Jan 2026)
- Injection test methodology — Andrej Karpathy
- Domain requirements — UC Berkeley DAB benchmark
