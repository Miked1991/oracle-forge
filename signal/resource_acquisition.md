# Resource Acquisition Report - Oracle Forge

## Current Resources

| Resource | Provider | Status | Access Level | Team Instructions |
|----------|----------|--------|-------------|-------------------|
| OpenRouter API | OpenRouter | Active (primary for DAB evaluation) | Team allocation; quota increase announced 2026-04-14 via cohort-wide all-broadcast | Primary LLM path for DAB runs. Configured via fork-first DAB workflow (Gemechis, commit `c4b3781`). Setup steps in forked DAB README. |
| Groq API | Groq | Active (secondary, KB testing only) | Free tier + team keys | Set `GROQ_API_KEY` env var. Used for KB injection tests — 21/21 pass on `llama-3.1-8b-instant` (`kb/INJECTION_TEST_LOG.md`). |

## Applications In Progress

| Resource | Provider | Date Applied | Status | Notes |
|----------|----------|-------------|--------|-------|
| DataAgentBench upstream contributor access | ucbepic/DataAgentBench | -- | Fork-first workflow adopted instead | Team forked DAB and runs evaluations against the fork (Gemechis, commit `99102de`). Upstream PR deferred to final submission once pass@1 is stable across datasets. |

## Evaluated but Not Pursued

| Resource | Provider | Decision | Reason | Alternative Used |
|----------|----------|----------|--------|------------------|
| Cloudflare Workers free tier | Cloudflare | Not pursued | Practitioner manual lists Cloudflare as "Option B" — a sandbox alternative, not a requirement. Inception §R6: *"Cloudflare is an upgrade, not a blocker."* | Local in-process simulated sandbox via `agent/sandbox_client.py` (`sandbox_mode: "simulated"`). No external sandbox service needed for DAB evaluation. |

## Priority Resources — Status Update

1. ~~**Cloudflare Workers**~~ — Deprioritized; local simulated sandbox sufficient for DAB eval (see Evaluated but Not Pursued).
2. ~~**LLM API Credits**~~ — Addressed via OpenRouter team allocation increase (2026-04-14). No further action needed pre-interim.
3. **DAB Community Access** — Pending. Required for final submission (Sat Apr 18) once pass@1 stabilizes across datasets.
4. **Compute** — Not currently blocking. Revisit only if OpenRouter latency or quota becomes a bottleneck during Week 9 stress runs.

## Instructions for Team

**OpenRouter (primary LLM, DAB evaluation):**
- API key configuration per Gemechis's fork-first DAB setup README (repo root).
- Follow setup guide in the forked DataAgentBench repo.

**Groq (KB injection tests only):**
- Set `GROQ_API_KEY` environment variable.
- Run `python kb/injection_test.py` — expected result: 21/21 pass against `llama-3.1-8b-instant`.

**Sandbox execution:**
- No external sandbox setup required. `agent/sandbox_client.py` executes plan steps in-process.
- Any team member can re-run evaluations from a fresh clone with `GROQ_API_KEY` + OpenRouter key in `.env`.
