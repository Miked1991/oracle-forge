# Claude Code Three-Layer Memory System

## The Pattern (from March 2026 source leak)

**Layer 1 - MEMORY.md (index):**

- Single file at repository root
- Contains: project structure pointers, key file locations, architectural decisions
- Never exceeds 500 lines
- Loaded at agent session start, remains in context

**Layer 2 - Topic Files (on-demand):**

- Loaded when MEMORY.md reference is encountered
- Each file: maximum 400 words, single responsibility
- Examples: DATABASE_SCHEMA.md, JOIN_STRATEGY.md, API_CONTRACTS.md
- NOT loaded until needed (prevents context pollution)

**Layer 3 - Session Transcripts (searchable):**

- Full interaction logs stored as .jsonl
- Agent searches transcripts when: "we solved this before"
- AutoDream consolidates transcripts into Layer 2 every 10 sessions

## Implementation for Oracle Forge

**Session start (always load):**

1. Load MEMORY.md (points to kb/architecture/*and kb/domain/*)
2. Load kb/domain/joins/join_key_mappings.md
3. Load kb/corrections/failure_log.md

**On query requiring specific DB:**
4. Load kb/domain/databases/[db_type]_schemas.md

**On join failure:**
5. Load kb/domain/joins/cross_db_join_patterns.md
6. Search kb/corrections/failure_by_category.md

## Critical Rules

- DO NOT load all documents at once (context window overflow)
- DO NOT skip MEMORY.md (agent loses navigation)
- DO NOT let any file exceed 400 words

## Injection Test

Q: What are the three layers of Claude Code's memory system?
A: MEMORY.md as index, topic files loaded on demand, session transcripts searchable
