# Oracle Forge — AI-DLC Inception Document
## Week 8 Sprint · TRP1 FDE Programme · April 2026

---

```
Document       : planning/inception_week8_oracle_forge.md
Status         : APPROVED — Gate 1 complete (Day 2 mob session)
Sprint         : Week 8 (Days 1–5)
Version        : v1.1
Created        : 2026-04-10
Authors        : Gemechis Urgessa & Eyor Alemu (Drivers)
Programme      : TRP1 FDE · Tenacious Intelligence Corp · 10 Academy
Gate approval  : APPROVED — 2026-04-11 (see Section 6 and planning/gate1_sequence_evidence.md)
```

---

## Team Composition

| Name     | Role                  | Primary Accountability                                         |
|----------|-----------------------|----------------------------------------------------------------|
| Gemechis | Driver                | Infrastructure setup, tenai-infra, MCP Toolbox, mob session lead |
| Eyor     | Driver                | Core agent build, evaluation harness, DAB benchmark submission |
| Gashaw   | Intelligence Officer  | KB v1 architecture docs, Claude Code & OpenAI source research  |
| Mikias   | Intelligence Officer  | KB v2 domain docs, join key glossary, adversarial probe library |
| Meseret  | Signal Corps          | X/Twitter threads, LinkedIn article, community engagement       |
| Kirubel  | Signal Corps          | Daily Slack updates, Cloudflare application, Reddit/Discord     |

> **Compounding principle:** Drivers without Intelligence Officers build in the dark.
> Intelligence Officers without Drivers produce knowledge that never ships.
> Both without Signal Corps produce work the world never sees.

---

## Table of Contents

1. [Press Release](#1-press-release)
2. [Honest FAQ — User Perspective](#2-honest-faq--user-perspective)
3. [Honest FAQ — Technical Perspective](#3-honest-faq--technical-perspective)
4. [Key Architectural Decisions](#4-key-architectural-decisions)
5. [Definition of Done](#5-definition-of-done)
6. [Gate 1 Approval Record](#6-gate-1-approval-record)
7. [Gate Q&A — Questions Raised and Answered by Team Members](#7-gate-qa--questions-raised-and-answered-by-team-members)
8. [Role Responsibilities This Sprint](#8-role-responsibilities-this-sprint)
9. [Risk Register](#9-risk-register)
10. [Dependencies and Blockers](#10-dependencies-and-blockers)
11. [Reference Documents](#11-reference-documents)
12. [Changelog](#12-changelog)

---

## 1. Press Release

> *Written in present tense as if the sprint is already complete.
> Hard to write well — that difficulty is the point.
> If this cannot be written clearly, the team does not yet agree on what it is building.*

---

**The Oracle Forge team has delivered a production-grade data analytics agent
that answers complex natural language business questions across PostgreSQL and
MongoDB enterprise databases with full query traceability.**

Built by Gemechis, Eyor, Gashaw, Mikias, Meseret, and Kirubel in five days, the
agent applies three engineering principles drawn from the most important AI
engineering disclosures of 2026: the Claude Code source architecture, OpenAI's
in-house data agent design, and the UC Berkeley DataAgentBench evaluation
framework.

The agent implements a three-layer context architecture — database schema
knowledge, institutional domain terminology, and a session-persistent
corrections log — that allows it to answer questions that no raw language model
can answer alone. It automatically resolves ill-formatted join keys between
integer-typed PostgreSQL customer IDs and CUST-prefixed MongoDB string IDs,
self-corrects failed queries through up to three retry attempts without
surfacing errors to the user, and returns every answer with a structured
`query_trace` that allows any result to be independently verified in under
thirty seconds.

The team's shared tenai-infra server with Tailscale mesh networking enables all
six members to co-pilot from any device in real time. An evaluation harness
adapted from the Week 5 Sentinel architecture produces a Week 8 baseline
pass@1 score against a held-out test set — the measurable foundation from which
Week 9 context engineering will drive the score above the current 38% benchmark
ceiling set by Gemini 3 Pro.

---

## 2. Honest FAQ — User Perspective

> *Questions a real user would ask about the finished product.
> Honest answers including what it does not do.
> This is not marketing — it is a commitment.*

---

### Q1: Can the agent answer any data question I type in plain English?

**A:** No. The agent handles questions that can be answered by querying the
configured PostgreSQL (Yelp dataset) and MongoDB (CRM dataset) databases using
SQL and MongoDB aggregation pipelines respectively.

It answers well: filtering, counting, aggregating, joining, ranking, and
comparing data across these two database types.

It will return inaccurate or empty answers for: questions requiring data from
SQLite or DuckDB datasets (not loaded until Week 9), questions involving complex
statistical modelling beyond SQL aggregation, and questions using business
terminology not yet documented in `kb/domain/terminology.md`. Questions outside
these boundaries receive a low-confidence flag (`"confidence": "low"`) in the
response rather than a confident wrong answer.

---

### Q2: How can I verify that an answer is actually correct?

**A:** Every answer includes a `query_trace` array in the response JSON. Each
element of the trace shows the exact SQL query or MongoDB aggregation pipeline
that was executed, the database it ran against, the number of rows returned, and
the execution time. You can copy any query from the trace and run it directly
in `psql` or `mongosh` to reproduce the result independently.

If an answer does not include a `query_trace`, do not trust it. The evaluation
harness only counts answers with valid traces as eligible for the pass@1 score.

---

### Q3: What will the agent definitely not handle correctly at the end of Week 8?

**A:** Three known gaps exist at end of Week 8:

1. **SQLite and DuckDB queries**: The agent routes to PostgreSQL and MongoDB
   only. Any question that requires data from a SQLite or DuckDB dataset will
   either return an error or be silently misrouted to PostgreSQL. Fix targeted
   for Week 9 Days 1–2.

2. **Unstructured text extraction**: Questions like "count support tickets
   mentioning billing issues in the notes field" require extracting structured
   facts from free-text fields. The Week 3 Document Intelligence pipeline
   integration is not built in Week 8. Targeted for Week 9 Days 3–4.

3. **Undocumented domain terms**: Business terms not in `kb/domain/terminology.md`
   are interpreted using generic industry definitions. "Active customer",
   "elite user", and "open business" may be interpreted incorrectly unless
   Gashaw and Mikias document their dataset-specific definitions before Day 3.

---

## 3. Honest FAQ — Technical Perspective

> *What could go wrong. What the hardest dependencies are.
> Honest answers that acknowledge real risk.*

---

### Q1: What is the single highest-risk technical component?

**A:** The PostgreSQL–MongoDB join key resolution. Customer entity IDs are
stored as plain integers in PostgreSQL (example: `1234`) but as prefixed
strings in MongoDB (example: `"CUST-1234"`). When the agent attempts a
cross-database join without resolving this format mismatch, the join returns
zero rows and produces no error — a silently wrong answer that appears
structurally valid.

**Mitigation in place:** Mikias documents the exact ID format for every loaded
dataset in `kb/domain/join_keys.md` before Day 3. The `executor.py`
self-correction loop flags any cross-database query that returns zero rows as
a likely join failure and retries with CUST-prefix stripping and integer
casting applied. This is DoD-6 and is verified independently.

---

### Q2: What happens if the Anthropic API is rate-limited or unavailable
during Days 3–5 construction?

**A:** Every agent step requiring LLM reasoning — query plan generation,
self-correction, answer summarisation — fails with a timeout. The agent returns
`{"error": "API_UNAVAILABLE", "confidence": 0, "query_trace": []}` rather than
hanging indefinitely. The evaluation harness counts this as a FAIL for that
query.

**Mitigation in place:** Eyor configures exponential backoff in the API client
(`ANTHROPIC_MAX_RETRIES=3`, delays of 10s, 30s, 90s). Heavy API construction
work is scheduled outside the 09:00–12:00 UTC peak window. Gemechis and Eyor
maintain a list of non-API tasks (infrastructure configuration, KB integration,
test set construction) to switch to if the API is unavailable for more than
two hours.

---

### Q3: What happens if a DAB dataset schema is not documented in the
Knowledge Base before the agent runs against it?

**A:** The agent introspects the live schema at query time using the MCP
Toolbox schema introspection tool. Without domain terminology in
`kb/domain/terminology.md`, ambiguous business terms use generic industry
definitions. Example: without documentation, the agent interprets "active
customer" as any row in the customers table — when the correct DAB definition
is "a customer with at least one order in the last 90 days". This produces a
plausible wrong answer.

**Mitigation in place:** Gashaw and Mikias document schema, join keys, and
terminology for the Yelp dataset before end of Day 2. Any term absent from the
KB causes the agent to append `"kb_gap_detected": true` to the response, so
Drivers can identify which answers need KB improvement.

---

### Q4: What if MongoDB or DuckDB installation fails or exceeds the Day 1–2
time budget?

**A:** The interim submission requires the agent to handle "at least two DAB
database types." PostgreSQL and SQLite satisfy this requirement without MongoDB
or DuckDB. If MongoDB installation takes more than four hours, Gemechis stops,
records the blocker in the risk log, and loads SQLite instead. The Week 8
Operations Document records the gap explicitly, and MongoDB becomes the first
task on Week 9 Day 1.

DuckDB is a Python package with no server — it installs in minutes. If it
fails, the most likely cause is a DAB setup script error, not a system issue.
Eyor reads the script output carefully before retrying.

---

### Q5: How does the team ensure the evaluation harness score is not inflated
by the team's own familiarity with the test questions?

**A:** Mikias constructs the held-out test set from DAB queries that the Drivers
have not seen during agent development. The test set JSON is committed to the
repository before Gemechis and Eyor run the agent against it for the first time.
The baseline score is recorded immediately after the first run with no
modifications between the test set being locked and the first score being taken.
This sequence is recorded in `eval/score_log/PROGRESS.md` with timestamps.

---

## 4. Key Architectural Decisions

> *Each decision with the chosen option and a one-sentence reason.
> No decision by default — every choice is explicit.*

---

### Decision 1: Database Interface Layer

| Field | Value |
|-------|-------|
| **Decision** | How the agent communicates with all four database types |
| **Option A** | Raw Python drivers per database: `psycopg2` (PostgreSQL), `pymongo` (MongoDB), `sqlite3` (SQLite), `duckdb` (DuckDB) |
| **Option B** | Google MCP Toolbox as a unified protocol interface layer |
| **Chosen** | **Option B — Google MCP Toolbox** |
| **Reason** | One `tools.yaml` file and one MCP protocol call replaces four different driver APIs, reduces agent code by ~60%, and means Gashaw and Mikias document one interface pattern in the KB instead of four. |
| **Owner** | Gemechis (configuration); Gashaw (KB documentation in `kb/architecture/`) |

---

### Decision 2: Database Loading Priority

| Field | Value |
|-------|-------|
| **Decision** | Order in which DAB database types are loaded and validated |
| **Option A** | Load all four database types simultaneously on Days 1–2 |
| **Option B** | Sequential priority: PostgreSQL → SQLite → MongoDB → DuckDB |
| **Chosen** | **Option B — Sequential, PostgreSQL first** |
| **Reason** | Most DAB queries target PostgreSQL; proving the full architecture on one database type before extending eliminates the risk of discovering a fundamental flaw on Day 5 with all four loaded. |
| **Owner** | Gemechis (loading); Eyor (validation with `eval/run_query.py` after each) |

---

### Decision 3: Code Execution Sandbox

| Field | Value |
|-------|-------|
| **Decision** | Where data transformation code runs outside the LLM context window |
| **Option A** | Local container via tenai-infra sandbox server on port 8080 |
| **Option B** | Cloudflare Workers free tier (cloud-hosted, public HTTPS URL) |
| **Chosen** | **Option A for Week 8; Option B if Kirubel confirms approval by Day 2** |
| **Reason** | Option A eliminates the external account approval dependency during the critical infrastructure window; switching to Option B in Week 9 if Cloudflare approves adds no Week 8 risk. |
| **Owner** | Gemechis (Option A setup); Kirubel (Cloudflare application Day 1) |

---

### Decision 4: Knowledge Base Construction Sequencing

| Field | Value |
|-------|-------|
| **Decision** | When KB documents are ready relative to the first agent code being written |
| **Option A** | KB built concurrently with agent code — both start on Day 3 |
| **Option B** | KB v1 (architecture) and KB v2 starter (Yelp schema + join keys) committed and injection-tested before Drivers write a single line of agent code |
| **Chosen** | **Option B — KB ready before agent code** |
| **Reason** | The agent's context loader reads KB at startup; building the agent without KB means the first test runs without context, producing misleading baseline scores and masking real architectural issues that KB would have prevented. |
| **Owner** | Gashaw and Mikias (KB committed by end of Day 2 mob session); Gemechis and Eyor (Construction begins Day 3) |

---

### Decision 5: Evaluation Harness Foundation

| Field | Value |
|-------|-------|
| **Decision** | Whether to build the evaluation harness from scratch or adapt Week 5 |
| **Option A** | Build a new harness tailored to DAB's specific JSON output format |
| **Option B** | Adapt the Week 5 Sentinel: same trace schema, scoring design, regression detection |
| **Chosen** | **Option B — Adapt the Week 5 Sentinel** |
| **Reason** | The challenge document explicitly states "this harness is The Sentinel applied to data agents — same trace schema, scoring design, regression detection"; adapting saves two days of harness development that would be spent building something the team has already built. |
| **Owner** | Eyor (harness implementation); Mikias (test set construction with verified ground-truth answers) |

---

## 5. Definition of Done

> *Numbered, specific, verifiable. Every item includes the exact verification
> command and the exact expected output. If you cannot verify an item in under
> 60 seconds, it is not specific enough — rewrite it.*
>
> **All 14 items must be verified live at the Day 5 Construction Gate mob session
> before the sprint is declared complete.**

---

### Infrastructure Group (Gemechis primary)

**DoD-1 — All six team devices connected to shared server via Tailscale**

```bash
# Run on team server:
tailscale status

# Expected: six devices listed with 100.x.x.x addresses
# Names: gemechis-*, eyor-*, gashaw-*, mikias-*, meseret-*, kirubel-*
# FAIL if any member's device is absent
```

**DoD-2 — tmux session 'oracle-forge' running and accessible from any device**

```bash
# On team server:
tmux list-sessions
# Expected output includes: oracle-forge: ...

# Each member verifies from their own device:
tmux attach -t oracle-forge
# Expected: shared terminal session visible
```

**DoD-3 — PostgreSQL loaded and Yelp query 0 returns correct structured JSON**

```bash
python eval/run_query.py --dataset yelp --query 0
# Expected:
# {
#   "answer": "150346",
#   "query_trace": [{"step":1, "database":"postgres_yelp",
#                    "sql":"SELECT COUNT(*) FROM businesses",
#                    "rows":1, "time_ms": <300}],
#   "confidence": >0.8
# }
# FAIL if answer is empty, error is present, or query_trace is absent
```

**DoD-4 — MCP Toolbox running and all configured database tools respond**

```bash
# List all tools:
curl http://localhost:5000/v1/tools | python3 -m json.tool | grep '"name"'
# Expected: "query_postgres" present (minimum)
# Advanced: all four tool names present

# Live query test:
curl -X POST http://localhost:5000/v1/tools/query_postgres/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM businesses"}'
# Expected: JSON with integer result, no error field
```

**DoD-5 — Code execution sandbox responding at configured URL**

```bash
curl -X POST http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{"code": "result = 2 + 2", "context": {}}'
# Expected:
# {"result": 4, "trace": [...],
#  "validation_status": "pass", "error_if_any": null}
```

### Agent Group (Eyor primary)

**DoD-6 — Agent answers a plain English question with full query_trace**

```bash
python3 -c "
from agent.agent import OracleForgeAgent
a = OracleForgeAgent()
r = a.answer_dab_format({
    'question': 'How many businesses are in the Yelp dataset?',
    'available_databases': ['postgres_yelp']
})
assert r['answer'] not in ['', None], 'Empty answer'
assert len(r['query_trace']) >= 1, 'No trace'
assert r['confidence'] > 0.5, 'Low confidence'
print('PASS — Answer:', r['answer'])
print('PASS — Trace steps:', len(r['query_trace']))
"
# Expected: PASS on all three assertions, answer close to 150346
# Time: under 15 seconds
```

**DoD-7 — Agent resolves PostgreSQL–MongoDB join key mismatch automatically**

```bash
# Requires MongoDB loaded (if not, mark as DEFERRED to Week 9 Day 1)
python3 tests/test_join_key_resolution.py
# Expected:
# Cross-DB query executed
# ID format conversion detected and applied (REPLACE/CAST in trace)
# Result rows: > 0
# FAIL if result rows = 0 when ground truth is non-zero
```

**DoD-8 — Agent self-corrects on a deliberately injected SQL syntax error**

```bash
python3 tests/test_self_correction.py
# Expected:
# Attempt 1: FAILED — injected syntax error triggered
# Attempt 2: SUCCEEDED — LLM-corrected query ran
# Final answer: [correct result]
# Attempts in query_trace: 2
# FAIL if no retry occurs or agent surfaces the error to the caller
```

### Knowledge Base Group (Gashaw and Mikias primary)

**DoD-9 — KB v1 has two architecture documents, each passing injection test**

```bash
ls kb/architecture/
# Expected files:
# claude_code_memory_system.md
# openai_data_agent_context.md
# CHANGELOG.md

# Injection test evidence in CHANGELOG:
grep "injection_test.*PASS" kb/architecture/CHANGELOG.md
# Expected: two lines, one per document
# FAIL if either document shows FAIL or is absent
```

**DoD-10 — KB v2 has Yelp schema, join keys, and terminology — each injection-tested**

```bash
ls kb/domain/
# Expected files:
# schemas/yelp_schema.md
# join_keys.md
# terminology.md
# CHANGELOG.md

grep "injection_test.*PASS" kb/domain/CHANGELOG.md
# Expected: three lines, one per document
# FAIL if any document is absent or shows injection test FAIL
```

### Evaluation Harness Group (Eyor primary, Mikias test set)

**DoD-11 — Test set has minimum 10 questions with verified correct answers**

```bash
python3 -c "
import json
cases = json.load(open('eval/test_set.json'))
assert len(cases) >= 10, f'Only {len(cases)} questions'
assert all('expected_answer' in c for c in cases), 'Missing expected_answer'
assert all('databases' in c for c in cases), 'Missing databases field'
assert all('query_id' in c for c in cases), 'Missing query_id'
print(f'PASS — {len(cases)} questions, all fields present')
"
```

**DoD-12 — Evaluation harness produces a Week 8 baseline score and commits it**

```bash
python eval/harness.py \
  --test-set eval/test_set.json \
  --output eval/score_log/week8_day5_baseline.json

# Expected output:
# Running yelp_001... PASS/FAIL
# ...
# SCORE: XX.X%  (N/10+)

# Verify score file committed:
git log --oneline eval/score_log/week8_day5_baseline.json | head -1
# Expected: commit hash and message visible
# FAIL if score file absent, or score = 0.0
```

### Signal Corps Group (Meseret and Kirubel primary)

**DoD-13 — First X/Twitter thread live with a specific named technical observation**

```bash
grep "Day 2.*X thread" signal/engagement_log.md
# Expected: URL to live post, specific technical observation named
# NOT acceptable: "We are building an AI agent"
# ACCEPTABLE: "We discovered PostgreSQL customer IDs are integers
#              but MongoDB stores them as CUST-[integer] strings —
#              a silent join failure that returns 0 rows with no error"
```

**DoD-14 — Daily Slack posts for Days 1–5 committed with links**

```bash
grep -c "Slack post" signal/engagement_log.md
# Expected: >= 5
# Each post: bullet list of (shipped, stuck, next, blockers)
```

## 6. Gate 1 Approval Record

> Completed at the Day 2 mob session. Full sequence evidence (dates vs. commits) is recorded in `planning/gate1_sequence_evidence.md`.

| Field | Value |
|-------|--------|
| **Gate status** | **APPROVED** |
| **Mob session date** | **2026-04-11** |
| **Session time** | **14:00–16:30 UTC** |
| **Session platform** | tenai-infra tmux + video call |
| **Document read by** | Gemechis (Driver), full document read aloud |

### Attendance and Approval

| Name     | Role                  | Present | Raised questions | Gave approval |
|----------|-----------------------|---------|------------------|---------------|
| Gemechis | Driver                | Yes | Yes (session logistics) | Yes |
| Eyor     | Driver                | Yes | Yes (DoD-12 baseline vs. press release — see Section 7 Q&A M-1) | Yes |
| Gashaw   | Intelligence Officer  | Yes | Yes (KB reading time — Section 7 G-1) | Yes |
| Mikias   | Intelligence Officer  | Yes | Yes (injection test removal criterion — Section 7 G-2) | Yes |
| Meseret  | Signal Corps          | Yes | Yes (**hardest question** — Section 7 M-1) | Yes |
| Kirubel  | Signal Corps          | Yes | Yes (Cloudflare “confirmed” threshold — Section 7 K-1) | Yes |

### Gate Outcome

| Field | Value |
|-------|--------|
| **Decision** | **APPROVED** — Construction authorised immediately after this session |
| **Construction authorised from** | **2026-04-11 16:30 UTC** |
| **Recorded by** | **Gemechis Urgessa (Driver)** |

### Hardest question (mob session) — required record

| | |
|--|--|
| **Asked by** | Meseret (Signal Corps) |
| **Question** | *“The Press Release says we surpass the 38% benchmark ceiling, but the Definition of Done only requires a baseline score above 0.10. How do we reconcile a strong public claim with a weak internal standard?”* (Section 7 — Question M-1) |
| **Answer summary** | Eyor: Week 8 baseline is a measurable starting point; the score that competes with the 38% ceiling is the post–Week 9 submission. DoD-12 is intentionally modest for Week 8. No change to Press Release; DoD-12 clarified accordingly. |

## 7. Gate Q&A — Questions Raised and Answered by Team Members

> *This section records every question raised during the Gate 1 mob session
> reading, who raised it, and the answer given. Questions from Signal Corps
> and Intelligence Officers are as important as questions from Drivers.
> The hardest question asked is the most valuable record in this document.*

### Q&A Block 1 — Questions from Meseret (Signal Corps)

**Question M-1**
> *"The Press Release says we surpass the 38% benchmark ceiling, but the
> Definition of Done only requires a baseline score above 0.10. How do we
> reconcile a strong public claim with a weak internal standard?"*
>
> — Meseret

**Answer (Eyor):**
The Press Release is written for the end of the two-week sprint — not the end of
Week 8. The Week 8 baseline can be any measurable score above zero because its
purpose is to provide a starting point, not to be the final score. The score
that competes against the 38% ceiling is the one submitted on April 18 after
Week 9 context engineering improvements. DoD-12 is intentionally modest for
Week 8 because we do not yet know what score is achievable — we find out by
running the harness.

**Decision:** No change to Press Release. DoD-12 wording updated to clarify:
"any score above 0.10 is acceptable for the Week 8 baseline; the April 18 final
score is the one that competes with 38%."

**Question M-2**
> *"I am supposed to post technical content about what we are building. But
> the Technical FAQ says MongoDB joins are a silent failure risk that we have
> not fully solved yet. Am I allowed to talk about known unsolved problems
> publicly, or do I only post about things that are working?"*
>
> — Meseret

**Answer (Gashaw):**
The challenge manual is explicit: "Describing a failure you diagnosed and fixed
is more credible than describing only successes. The technical community reads
for learning, not marketing." You are not only allowed to post about unsolved
problems — you are expected to. The honest technical post about discovering the
PostgreSQL integer vs MongoDB CUST-string join failure is more valuable to the
data engineering community than any announcement of a working product. Post the
problem as you discover it. Post the fix when you have it.

**Decision:** No document change. Meseret adds this specific failure mode to her
Day 2 X thread topic list in `signal/engagement_log.md` planning notes.

### Q&A Block 2 — Questions from Kirubel (Signal Corps)

**Question K-1**
> *"Decision 3 says we default to Option A (local container sandbox) but
> switch to Cloudflare Workers if I confirm approval by Day 2. What exactly
> does 'confirmed' mean — do I need written approval, a login credential, or
> just an account created?"*
>
> — Kirubel

**Answer (Gemechis):**
Confirmed means: you have a Cloudflare account, you have installed Wrangler CLI,
you have run `wrangler login` successfully, and you have attempted `wrangler deploy`
from the `workers/` directory at least once — even if the first deploy fails.
We need to know whether the Cloudflare pathway is actually viable before
committing to it. If you have a working deploy URL by the Day 2 mob session,
we switch. If not, we stay on Option A and move on. Either outcome is fine.

**Decision:** Decision 3 updated to clarify the "confirmed" threshold. Kirubel
adds a note to complete the Wrangler test on Day 1 and report the outcome at
the start of the Day 2 mob session.

**Question K-2**
> *"The daily Slack post is listed as a Signal Corps deliverable, but the
> challenge rubric says 'repo has commits from all six members.' I do not write
> code. How do I get commits into the repository so the rubric is satisfied?"*
>
> — Kirubel

**Answer (Eyor):**
Signal Corps commits are real commits to real files in the repository.
`signal/engagement_log.md` is a markdown file that Kirubel and Meseret update
daily. Each daily update is a commit from that member's Git identity. Additionally,
Kirubel owns `signal/community_log.md` and the Cloudflare access instructions in
`signal/resource_acquisition.md`. These are all committed content. The rubric
requirement is satisfied by daily commits to Signal Corps files — you do not
need to commit code.

**Decision:** Signal Corps commit workflow documented in
`signal/CONTRIBUTING.md`. Kirubel sets up Git identity on the shared server on
Day 1.

### Q&A Block 3 — Questions from Gashaw (Intelligence Officer)

**Question G-1**
> *"DoD-9 requires KB v1 documents to pass injection tests before Day 3.
> But I need to study the Claude Code source repository and the OpenAI
> writeup before I can write the documents. How much reading time is built into
> the plan? I want to make sure the documents are actually accurate and not
> just structurally complete."*
>
> — Gashaw

**Answer (Gemechis):**
Days 1 and 2 are your primary KB construction window. Day 1 is reading and note-taking.
Day 2 is writing, injection testing, and committing. That is approximately eight
hours of work across two days for two documents — roughly four hours per document
including reading, writing, and testing. This is tight but achievable if the
documents are kept under 400 words each as the Karpathy method requires. The
injection test is fast: paste the document into a fresh Claude session, ask one
question, grade the answer. If Day 2 runs long and only one document is
injection-tested, the second document should be tested first thing on Day 3
before Gemechis and Eyor begin writing agent code.

**Decision:** DoD-9 updated to allow a grace period: "both documents must be
committed by end of Day 2; if injection test for the second document is not
complete by Day 2 end, it must be completed and passed before Drivers begin
writing agent code on Day 3, even if this means a brief delay to the Day 3
start."

**Question G-2**
> *"The 'Specific to this problem' KB quality criterion says to remove everything
> the LLM already knows from pretraining. But how do I know what Claude already
> knows? If I remove too much I leave the document too thin. If I leave too much
> the document is noise."*
>
> — Gashaw

**Answer (Mikias):**
The practical test is: open a fresh Claude session with no document and ask the
question. If Claude gives the correct specific answer without the document —
remove that information from the document. If Claude gives a wrong or generic
answer without the document — that information must stay. For example, Claude
knows what PostgreSQL is, so we do not explain it. But Claude does not know
that the Yelp dataset's `businesses.stars` is a FLOAT average and NOT an integer
count — that must be in the document. The injection test is also the removal
test: if the document fails injection, the information is not specific enough
to be worth keeping.

**Decision:** Gashaw adds a "what Claude doesn't know" column to each KB
document's planning notes before writing. No change to the Inception Document.

### Q&A Block 4 — Questions from Mikias (Intelligence Officer)

**Question Mi-1**
> *"I am responsible for building the adversarial probe library. The challenge
> requires 15 probes across at least 3 failure categories. Is this Week 8
> work or Week 9 work? I do not see it in the Definition of Done for Week 8."*
>
> — Mikias

**Answer (Eyor):**
The adversarial probe library is Week 9 work. In Week 8, your primary KB
deliverables are DoD-9 and DoD-10 — architecture docs and domain docs. The
Week 8 contribution to the probe library is your `eval/test_set.json` (DoD-11):
the 10+ test questions you build now become the seed for the adversarial probe
library in Week 9. Every question in the test set that the agent fails in Week 8
is a candidate probe for Week 9. Document each failure in
`kb/corrections/corrections_log.md` as you observe them during the Day 5
baseline run.

**Decision:** No change to DoD. Mikias adds a note to Day 5 tasks:
"During baseline harness run, log every FAIL result to corrections_log.md with
the query, what the agent returned, and what the correct answer is."

**Question Mi-2**
> *"The join key glossary (kb/domain/join_keys.md) requires me to inspect
> actual MongoDB records to find out how customer IDs are formatted. But MongoDB
> may not be installed until late on Day 1 or even Day 2. What do I write in
> the document before I can see the actual records?"*
>
> — Mikias

**Answer (Gemechis):**
Write a placeholder structure with the field names and the question to answer:
`customer_id format: [TO FILL — inspect with mongosh db.customers.findOne()]`.
The document is not considered complete for DoD-10 until the actual format
strings are filled in. This is fine — it means the document exists and is
structured correctly, and the one missing line is added the moment MongoDB is
accessible. Gashaw and Mikias: when MongoDB is first loaded, the first thing
either of you does is run `mongosh`, inspect one customer record, and fill in
the format strings. Do not wait until later.

**Decision:** DoD-10 updated with a note: "join_keys.md is not considered
injection-testable until MongoDB is loaded and actual ID format strings are
documented. If MongoDB is not loaded by end of Day 2, mark DoD-10 as PARTIAL
with join_keys.md noted as pending, and complete it as the first task after
MongoDB loads."

### Q&A Block 5 — Questions from Eyor (Driver)

**Question E-1**
> *"The Press Release says we handle PostgreSQL and MongoDB in Week 8. But
> Key Decision 2 says we load PostgreSQL first and SQLite second. We might
> not have MongoDB running at all by the time the press release would be
> accurate. Should the Press Release say PostgreSQL and SQLite instead?"*
>
> — Eyor

**Answer (Gemechis):**
The Press Release describes the aspirational end state of the sprint —
what we are committing to deliver, not what we have delivered on Day 1.
If we load PostgreSQL and SQLite first but MongoDB is our intended
second database type for the agent (because the challenge's hardest test
is the cross-DB join between PostgreSQL and MongoDB), then the Press Release
is correct in naming MongoDB. The loading order in Decision 2 is for risk
management — we start with the most important database — but the sprint
goal is to have MongoDB running and cross-database queries working by
Day 5. If MongoDB is not working by Day 5, the Press Release becomes
inaccurate and we must update it in the Operations Document.

**Decision:** Press Release remains unchanged. DoD-7 (cross-DB join
resolution) is marked as "(requires MongoDB loaded — if not, mark DEFERRED
to Week 9 Day 1)" to acknowledge the dependency explicitly.

**Question E-2**
> *"DoD-8 tests self-correction using 'a deliberately injected SQL syntax
> error.' Who writes that test? It needs to exist before Day 5 for me to
> run it. Is that something I build or something Mikias builds for the
> test set?"*
>
> — Eyor

**Answer (Mikias):**
I will write `tests/test_self_correction.py` as part of the shared utility
library. It will inject a known-bad query (e.g. `SELECT FORM businesses`
— typo in FROM) through the executor and verify that: (1) attempt 1 fails,
(2) the LLM produces a corrected query, (3) attempt 2 succeeds, and (4) the
`query_trace` contains two attempts. I will commit this by end of Day 4
so Eyor can run it on Day 5 for the gate.

**Decision:** `tests/test_self_correction.py` added to Mikias's Day 4 task
list. Eyor notes this as a hard dependency for DoD-8 verification.

### Q&A Block 6 — Questions from Gemechis (Driver)

**Question Ge-1**
> *"The risk register lists R3 as 'KB documents not ready before Day 3
> agent build begins' with Low probability because the gate enforces it.
> But the gate only requires DoD-9 and DoD-10 to be complete — it does not
> require me and Eyor to have actually read the KB documents. What is the
> point of having the KB ready if the Drivers have not read it?"*
>
> — Gemechis

**Answer (Gashaw):**
That is a real gap. The KB documents are only useful if you use them. Adding
a formal reading step: before the Day 3 mob session begins, Gashaw presents
a five-minute summary of each KB v1 document to Gemechis and Eyor. This is
not optional reading for Drivers — it is the IO update segment of the Day 3
mob session. The 10-minute IO update at the start of every mob session is
exactly the mechanism for this. On Day 3, the IO update covers the KB
documents, not ecosystem news.

**Decision:** Day 3 mob session agenda updated: IO update segment covers
KB v1 and KB v2 document summaries for Drivers, not general ecosystem news.
This is recorded in the sprint plan in Section 8.

---

**Question Ge-2**
> *"This is the hardest question I can think of for this Inception Document:
> if we run the harness on Day 5, get a baseline score, and the score is 10%,
> does the team consider Week 8 a failure? What is the minimum acceptable
> outcome for this sprint?"*
>
> — Gemechis

**Answer (full team discussion):**
A 10% baseline is not a failure — it is a measurement. The Week 8 sprint's
definition of success is: infrastructure running, agent running, harness
producing a score, and KB v1/v2 committed and injection-tested. The score
number is the starting line, not the finish line. A team that has a 10%
baseline on Day 5 but a complete, correctly structured harness with query
traces for every tested question is in a better position for Week 9 than a
team that has a 35% baseline but no query traces and no KB to improve against.
The challenge manual states: "the harness that produced the score is the real
deliverable." A 10% score with full traceability is a real deliverable.
A 35% score with no traceability is not.

**Decision:** This answer is recorded as the formal interpretation of
DoD-12. The minimum acceptable Week 8 outcome is: score > 0%, harness
produces query traces for every tested question, score committed with
timestamp. The Operations Document will record the actual score honestly,
whatever it is.

> **Note:** This is the hardest question asked in this gate session.
> It is recorded here because it is the question most likely to come up
> again at the Week 9 gate and the final submission review.

## 8. Role Responsibilities This Sprint

### Drivers — Gemechis and Eyor

**Mob session role:** Gemechis holds keyboard during 40-minute construction
segment. Eyor is primary co-pilot — calls out issues, reads KB documents for
conflicts, runs verification commands independently.

```markdown
| Day | Gemechis | Eyor |
|-----|----------|------|
| 1 (AM) | Clone tenai-infra to `/shared/tenai-infra`; read README completely | Load PostgreSQL datasets; run `python eval/run_query.py --dataset yelp --query 0` |
| 1 (PM) | Configure Tailscale auth keys; confirm all 6 devices connect | Install DAB dependencies; verify `python eval/list_datasets.py` |
| 2 (AM) | Download MCP Toolbox binary; create `mcp/tools.yaml` from template | Start sandbox server; test `/execute` endpoint |
| 2 (PM) | Start MCP Toolbox server; verify all tools respond; load SQLite | Set up team `.env` file; configure API key with retry settings |
| 2 (mob) | **Lead Inception Gate reading — every section aloud** | Run DoD verification commands during gate; record answers |
| 3 (AM) | Build `agent/agent.py` — main class, context loader, `answer_dab_format()` | Build `agent/router.py` — multi-database keyword routing |
| 3 (PM) | Wire agent to MCP Toolbox; test end-to-end on Yelp query | Build `agent/executor.py` — query execution with retry |
| 4 (AM) | Build self-correction loop in `executor.py`; test with injected failures | Adapt Week 5 Sentinel for DAB harness (`eval/harness.py`) |
| 4 (PM) | Load MongoDB; inspect records; confirm join key format with Mikias | Build `eval/test_set.json` minimum 10 questions with Mikias |
| 5 (AM) | End-to-end agent test on 5+ Yelp questions; fix routing errors | **Run harness; record baseline score; commit to `eval/score_log/`** |
| 5 (mob) | **Lead Construction Gate — run every DoD verification command live** | Record gate results in `planning/construction_gate_week8.md` |
```

### Intelligence Officers — Gashaw and Mikias

**Mob session role:** 10-minute IO update opens every session. Day 3
update covers KB documents specifically (not ecosystem news) for Drivers.

```markdown
| Day | Gashaw | Mikias |
|-----|--------|--------|
| 1 | Study Claude Code source analysis: `github.com/sanbuphy/claude-code-source-code` docs/en/ — extract three-layer MEMORY.md, autoDream, tool scoping | Study OpenAI data agent writeup: `openai.com/index/inside-our-in-house-data-agent` — extract six-layer context, self-correction pattern |
| 2 (AM) | Write `kb/architecture/claude_code_memory_system.md` (max 400 words) | Write `kb/architecture/openai_data_agent_context.md` (max 400 words) |
| 2 (PM) | Run injection test for claude_code_memory_system.md; commit PASS | Write `kb/domain/schemas/yelp_schema.md`; inspect PostgreSQL schema live |
| 2 (mob) | Run injection test for openai_data_agent_context.md; commit PASS | Write `kb/domain/join_keys.md` placeholder; inspect MongoDB records when available |
| 3 | Write `kb/domain/terminology.md` — yelp business terms with exact definitions | Write `kb/evaluation/dab_scoring.md` — pass@1 method, 54 queries, submission format |
| 3 (mob) | **Present KB v1 and v2 summaries to Drivers — Day 3 IO update** | Injection test all three domain documents; record in CHANGELOG |
| 4 | Build `utils/kb_loader.py` — reusable KB loading module with tests | Build `utils/join_resolver.py` — format detection and conversion; write `tests/test_self_correction.py` |
| 5 | Build `utils/schema_introspector.py`; write Week 8 global ecosystem report | Finalise `eval/test_set.json` (10+ questions, verified answers); observe baseline harness run; log all FAIL entries to `kb/corrections/corrections_log.md` |
```

**KB injection test protocol (mandatory before every commit):**

```markdown
1. Open a fresh Claude session — zero prior context
2. Paste only the document as the first message
3. Ask the test question written in the document's front matter
4. Grade: specific correct answer = PASS → commit with CHANGELOG entry
          vague or wrong answer   = FAIL → revise, do not commit
5. Record in CHANGELOG.md: date, test question asked,
   answer received, verdict (PASS/FAIL)
```

### Signal Corps — Meseret and Kirubel

**Mob session role:** 10-minute Signal Corps update opens every session.
Report what went out, responses received, resources obtained, and any
community intelligence that changes the technical approach.

```markdown
| Day | Meseret | Kirubel |
|-----|---------|---------|
| 1 | Identify 5 X/Twitter accounts posting about data agents, Claude Code, or DAB benchmarks. Subscribe to DAB repository. | **Apply for Cloudflare Workers free tier.** Test `wrangler login`. Report outcome at mob session. Confirm Git identity on server. |
| 2 | **Post first X thread:** comment on a notable Claude Code architecture or data agent post. Include the specific PostgreSQL/MongoDB join key failure observation from Gashaw's research. Name the exact technical detail. | Post Day 1 + Day 2 Slack updates (4 bullets each: shipped, stuck, next, blockers). Save Cloudflare outcome in `signal/resource_acquisition.md`. |
| 3 | Post one substantive Reddit comment on r/MachineLearning or r/LocalLLaMA. Save link in `signal/community_log.md`. | Post Day 3 Slack update. Note any community intelligence from Reddit/Discord that changes technical approach. |
| 4 | **Post second X thread:** describe what the team is building — the DAB benchmark, the multi-DB architecture decision, something unexpected from Day 3 infrastructure. In-progress engineering posts outperform announcements. | Post Day 4 Slack update. |
| 5 | **Compile Week 8 engagement summary.** All post URLs, response metrics, resources obtained. Report at mob session. Identify any external technical intelligence that changes Week 9 plan. | Post Day 5 Slack update. Commit all `signal/` files. Ensure `engagement_log.md` has entries for all 5 days. |
```

**Signal Corps content standard — enforced:**

```markdown
| Not acceptable | Acceptable |
|---|---|
| "We are building an AI agent." | "We found that PostgreSQL customer IDs are plain integers but MongoDB stores them as CUST-[integer] strings. A JOIN without converting the format returns 0 rows silently — no error, just a wrong answer." |
| "We improved our multi-database handling." | "Our agent's pass@1 score went from 0% to 31% in 48 hours — purely by loading the Knowledge Base into context. No model change. Just better context." |
| "Excited to be working on this challenge!" | "We are adapting the Week 5 Sentinel evaluation architecture for a new domain: data agents. Same trace schema, different target. Here is what changed and why." |
```

## 9. Risk Register

```markdown
| ID | Risk Description | Probability | Impact | Mitigation | Owner |
|----|-----------------|-------------|--------|-----------|-------|
| R1 | MongoDB installation fails or exceeds 4-hour budget on Day 1 | Medium | High — blocks cross-DB join testing and DoD-7 | Fall back to PostgreSQL + SQLite for Week 8. Record gap in Operations. MongoDB becomes first task Week 9 Day 1. | Gemechis |
| R2 | Anthropic API rate-limited during Days 3–5 peak construction hours | Low–Medium | High — blocks all LLM-dependent development | Set `ANTHROPIC_MAX_RETRIES=3`; schedule heavy API work outside 09:00–12:00 UTC; maintain list of non-API tasks to switch to | Eyor |
| R3 | KB v1 or KB v2 documents not injection-tested before Day 3 agent build | Low (gate enforces this) | High — first agent runs without context; baseline score misleads | Gate approval enforces KB completion. Gashaw and Mikias commit KB before end of Day 2 mob session. DoD-9/10 cannot be approved otherwise. | Gashaw |
| R4 | Silent cross-database join failure (zero rows, no error) | High — known architectural risk | High — directly reduces benchmark score | `executor.py` retries any cross-DB join returning zero rows; `join_keys.md` documents all known format pairs; DoD-7 tests this specifically | Mikias |
| R5 | Tailscale fails to connect for one or more team members' devices | Low | Medium — reduces mob participation quality | Primary workaround: video call screen share as temporary substitute; fix Tailscale in parallel. All members must test on Day 1. | Kirubel |
| R6 | Cloudflare Workers not approved by Day 2 | Medium | Low — Option A sandbox fully sufficient | Default to Option A throughout Week 8. Cloudflare is an upgrade, not a blocker. | Kirubel |
| R7 | MCP Toolbox version incompatibility with installed database versions | Low | Medium — delays database integration by up to 1 day | Check `googleapis/genai-toolbox` releases on Day 1 for known compatibility issues. Test against PostgreSQL before configuring other DBs. | Eyor |
| R8 | Week 8 baseline score is zero due to agent configuration error | Low | Medium — Week 8 Operations Document reports zero progress | Run a manual single-query test (`python3 -c "..."`) before running full harness. Confirm at least one answer is non-empty. | Eyor |
```

## 10. Dependencies and Blockers

### Hard dependencies — must be resolved before Construction begins on Day 3

```markdown
| Dependency | Needed by | Status | Owner | Escalation if blocked |
|------------|-----------|--------|-------|----------------------|
| tenai-infra repository URL | Day 1 infrastructure | ❌ Outstanding | Gemechis | Ask facilitator at programme start |
| Anthropic API key with sufficient quota for 2-week sprint | Day 3 agent build | ❌ Outstanding | Eyor | Request from programme coordinator |
| Tailscale auth key for team network | Day 1 team connectivity | ❌ Outstanding | Gemechis | Request from facilitator with tenai-infra URL |
| DataAgentBench repository access | Day 1 database loading | ✅ Public GitHub | Eyor | N/A — public repo |
| MCP Toolbox binary | Day 2 MCP config | ✅ Available on googleapis | Gemechis | N/A — public download |
```

### Soft dependencies — workarounds available

```markdown
| Dependency | Workaround if unavailable |
|------------|--------------------------|
| Cloudflare Workers approval (Kirubel Day 1) | Use Option A local container throughout — equal functionality |
| MongoDB installation on Day 1 | PostgreSQL + SQLite satisfy interim submission minimum of 2 DB types |
| All six devices on Tailscale | Video call screen share for mob session; fix Tailscale asynchronously |
| DuckDB datasets | Not needed for Week 8 baseline; defer to Week 9 Day 1–2 |
```

## 11. Reference Documents

```markdown
| Resource | URL | Must read before |
|----------|-----|-----------------|
| DataAgentBench repository | `github.com/ucbepic/DataAgentBench` | Day 1 (Gashaw, Mikias, Eyor) |
| DataAgentBench paper | `arxiv.org/html/2603.20576` | Day 1 (Gashaw, Mikias) |
| Google MCP Toolbox | `github.com/googleapis/genai-toolbox` | Day 1 (Gemechis, Eyor) |
| Claude Code architecture analyses | `github.com/sanbuphy/claude-code-source-code` (docs/en/) | Day 1 (Gashaw) |
| OpenAI in-house data agent writeup | `openai.com/index/inside-our-in-house-data-agent` | Day 1 (Mikias) |
| Karpathy LLM Knowledge Bases | `academy.dair.ai/blog/llm-knowledge-bases-karpathy` | Day 1 (Gashaw, Mikias) |
| AWS AI-DLC framework | `aws.amazon.com/blogs/devops/ai-driven-development-life-cycle/` | Day 1 (Gemechis, Eyor) |
| AI Agent Internals Probing Strategy | `AI_Agents_Internal_Probing_Strategy.docx` in challenge package | Day 2 (Gashaw, Mikias) |
| Cloudflare Workers free tier | `workers.cloudflare.com` | Day 1 (Kirubel) |
```

## 12. Changelog

| Version | Date | Author | Change Summary |
|---------|------|--------|----------------|
| v1.0 | 2026-04-10 | Gemechis, Eyor | Initial draft — all sections complete |
| v1.1 | [gate date] | Full team | Post-gate revisions from Section 7 Q&A: DoD-9 grace period added; DoD-10 partial completion noted; DoD-12 minimum acceptable outcome clarified; DoD-7 MongoDB dependency noted; Decision 3 confirmation threshold clarified; KB reading step added to Day 3 mob agenda |
| v1.2 | [gate date] | Gemechis | Final approved version — gate outcome recorded in Section 6 |

```markdown
╔══════════════════════════════════════════════════════════════════╗
║  GATE 1 STATUS: PENDING                                          ║
║                                                                  ║
║  This document is ACTIVE only after all six members have         ║
║  explicitly approved it at a mob session (Section 6).            ║
║                                                                  ║
║  NO agent code may be written before Gate 1 is APPROVED.         ║
║                                                                  ║
║  Construction authorised from: ___________________________       ║
╚══════════════════════════════════════════════════════════════════╝
```

*TRP1 FDE Programme · Tenacious Intelligence Corp · 10 Academy · April 2026*
*File: `planning/inception_week8_oracle_forge.md`*
