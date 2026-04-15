# Oracle Forge Knowledge Base — Team Guide

## What This KB Is

This is the **agent's brain**. Not documentation for humans (though humans maintain it). Every file here is designed to be injected directly into an LLM context window to give the agent working knowledge.

## Directory Structure

```
kb/
├── architecture/          # How the agent thinks and is structured
│   ├── memory.md                      # Three-layer memory system
│   ├── autodream_consolidation.md     # Weekly session compression pattern
│   ├── tool_scoping_philosophy.md     # 40+ tight tools > 5 generic tools
│   ├── openai_layers.md               # Six-layer context architecture
│   ├── conductor_worker_pattern.md    # Multi-database routing
│   └── evaluation_harness_schema.md   # Trace schema + pass@1 scoring
│
├── domain/                # DAB dataset-specific knowledge
│   ├── databases/
│   │   ├── postgresql_schemas.md      # Yelp, Telecom, Healthcare schemas
│   │   ├── mongodb_schemas.md         # Nested document structures
│   │   ├── sqlite_schemas.md          # Lightweight transaction schemas
│   │   └── duckdb_schemas.md          # Analytical columnar schemas
│   ├── joins/
│   │   ├── join_key_mappings.md       # Cross-DB format transformations
│   │   └── cross_db_join_patterns.md  # PG → Mongo join step-by-step
│   ├── unstructured/
│   │   ├── text_extraction_patterns.md  # Regex + NLP extraction
│   │   └── sentiment_mapping.md       # Sentiment lexicon with negation
│   └── domain_terms/
│       └── business_glossary.md       # Term definitions by dataset
│
├── corrections/           # Self-correction loop (autoDream output)
│   ├── failure_log.md                 # Chronological failures + fixes
│   ├── failure_by_category.md         # Failures by DAB's 4 categories
│   ├── resolved_patterns.md           # Permanent fixes with confidence scores
│   └── regression_prevention.md       # Regression test rules
│
└── evaluation/            # DAB benchmark reference
    ├── dab_scoring_method.md          # pass@1 definition and calculation
    ├── submission_format.md           # PR requirements + AGENT.md template
    └── dab_format.md                  # Full evaluation reference (supporting)
```

## The Karpathy Discipline

**Rule 1: Injection-test every file.** Run `python injection_test.py` from the repo root. If a document fails, revise it until it passes — do not merge failing documents.

**Rule 2: Remove before adding.** Growth without removal = noise. Every addition should trigger a review of what can be removed.

**Rule 3: Version everything.** Each directory has a `CHANGELOG.md` that tracks what changed, why, and the injection test result.

## Who Does What

| Role | KB Responsibility |
|------|-------------------|
| Intelligence Officer | Maintain all files. Run injection tests. Compile from raw sources. |
| Driver | Inject relevant files at session start. Log failures to `corrections/failure_log.md`. |
| Signal Corps | Validate KB claims against external sources. Surface community corrections. |

## Session Start — Load Order

Inject files in this order at the start of each agent session:

1. `architecture/memory.md`
2. `architecture/conductor_worker_pattern.md`
3. `architecture/openai_layers.md`
4. `corrections/failure_log.md`
5. `corrections/resolved_patterns.md`

Then load domain-specific files as needed for the active query:

- Schema files from `domain/databases/` for each DB type in use
- `domain/joins/join_key_mappings.md` whenever a cross-DB join is required
- `domain/domain_terms/business_glossary.md` for telecom/Yelp/healthcare queries

## Change Workflow

1. Intelligence Officer drafts change
2. Runs `python injection_test.py --test-single <path>` locally
3. If passes, commits with a CHANGELOG entry in the relevant subdirectory
4. Announces at next mob session
5. Team reviews for unintended consequences
