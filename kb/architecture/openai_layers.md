# OpenAI Data Agent Six-Layer Context Architecture

## The Six Layers (from Jan 2026 writeup)

**Layer 1 - Schema & Metadata:** All table schemas, relationships, indexes. Loaded at session start, never evicted. For DAB: 12 datasets × 4 DB types.

**Layer 2 - Institutional Knowledge:** Which tables are authoritative, data quality notes. Our kb/domain/domain_terms/

**Layer 3 - Interaction Memory:** Corrections from previous queries. Our kb/corrections/

**Layer 4 - Query Patterns:** Successful SQL/MongoDB patterns from prior runs.

**Layer 5 - Codex-Powered Table Enrichment:** For tables > 100 columns: generate semantic summaries.

**Layer 6 - Closed-Loop Self-Correction:** Execute → validate → on failure → retrieve similar correction → retry. Max 3 retries.

## Minimum for Oracle Forge (3 layers that demonstrably work)

**Layer A (Schema):** Load once per DB type used in session
**Layer B (Institutional):** kb/domain/joins + terms (always loaded)
**Layer C (Correction / correction log):** kb/corrections/failure_log.md (always loaded)

**Success metric:** Agent resolves join key mismatch without being explicitly told.

## Injection Test

Q: What are the minimum three context layers for Oracle Forge?
A: Schema, institutional (joins/terms), correction log
