# Epic: Data Collection — Staged Deliverables

*Created: 2026-04-14*
*Epic: [epic-data-collection.md](epic-data-collection.md)*

---

## Delivery Structure

Four parallel tracks after a shared infrastructure gate. Infrastructure must reach Stage I-2 before any collector work begins.

```
INFRA ──► I-1 ──► I-2 ──► I-3 (entry point + docs, non-blocking)
                  │
          ┌───────┼────────────┐
          ▼       ▼            ▼
       C1 track  C2 track    C3 track
       (5 stages)(5 stages)  (4 stages)
          │       │            │
          └───────┴────────────┘
                  │
           Integration smoke test
           (run_collectors.ps1 end-to-end)
```

---

## Track I: Shared Infrastructure

> Foundation that all three collector tracks depend on. Build first.

### I-1 — DB Schema + Config Loader
**Complexity: M** | **Unblocks: all three collector tracks**

**Artifacts:**
- `init_db.py` — idempotent `CREATE TABLE IF NOT EXISTS` for all three DBs (`sessions.db`, `github.db`, `appswitch.db`)
- `config.yaml` — canonical config file with all fields documented and defaults set
- `config_loader.py` — validates `config.yaml`, raises on missing required fields, exposes a typed config object

**Acceptance criteria:**
- [ ] `python init_db.py` is safe to run twice; no errors, no duplicate tables
- [ ] Deleting DB files and re-running recreates them correctly
- [ ] `config_loader.py` returns correct defaults when optional fields absent
- [ ] `config_loader.py` raises clearly when a required field is missing

---

### I-2 — Health Writer + Health Check
**Complexity: S** | **Unblocks: all collectors can add `write_health()` calls independently**

**Artifacts:**
- `health_writer.py` — `write_health(collector_name, record_count)` atomically updates `data/health.json`; merges rather than overwrites
- `health_check.py` — standalone + importable; reads `health.json`, warns on any collector stale >48h; exits 1 if any stale, 0 if all healthy

**`health.json` schema:**
```json
{
  "session_parser":   { "last_success": "<ISO8601>", "last_record_count": 0 },
  "github_poller":    { "last_success": "<ISO8601>", "last_record_count": 0 },
  "appswitch_export": { "last_success": "<ISO8601>", "last_record_count": 0 }
}
```

**Acceptance criteria:**
- [ ] `write_health("session_parser", 42)` creates or updates `health.json` without corrupting other entries
- [ ] Calling `write_health` twice for the same collector updates, not duplicates
- [ ] `health_check.py` exits 1 when any `last_success` is >48h ago
- [ ] `health_check.py` exits 1 (not crash) when `health.json` does not exist
- [ ] `from health_check import check_health` works without side effects

---

### I-3 — Entry Point + Setup Docs
**Complexity: S** | **Non-blocking — can be built in parallel with collector tracks**

**Artifacts:**
- `run_collectors.ps1` — runs all three collectors in sequence; redirects stdout/stderr to dated log under `logs/`; calls `health_check.py` at end
- `setup.md` — exactly three steps: (1) register Claude Code hooks, (2) add Task Scheduler entry pointing at `run_collectors.ps1`, (3) edit `config.yaml`

**Acceptance criteria:**
- [ ] Running `run_collectors.ps1` with stub collector scripts produces a dated log file
- [ ] A failing collector logs the error and does not abort subsequent collectors
- [ ] `setup.md` is self-contained; a person with no prior context can complete setup from it alone

---

## Track C1: Session Parser

> Depends on I-1, I-2. Prior art: ccusage (parsing), disler/hooks-observability (hook schema).

### C1-1 — JSONL Parser + Fixture Harness
**Complexity: S**

**Artifacts:**
- `collector/session_parser.py` — core parsing module (no DB, no hook); adapted from ccusage; extracts: turn count, tool call count, tool failure count, session duration, working directory, git branch (from working dir via `git rev-parse --abbrev-ref HEAD`), raw per-turn message list, and `raw_content_json` (session JSON with `type: thinking`, `type: tool_use`, and `type: tool_result` blocks stripped — leaving only user and assistant text turns)
- `tests/fixtures/sample_session.jsonl` — canonical test fixture (real or hand-crafted)
- `tests/test_parser.py` — unit tests asserting exact field values against fixture

**Acceptance criteria:**
- [ ] `pytest tests/test_parser.py` passes with zero failures
- [ ] Parser returns correct values for all fields against the fixture, including `git_branch` and `raw_content_json`
- [ ] `raw_content_json` contains no `type: thinking`, `type: tool_use`, or `type: tool_result` blocks; user and assistant text turns preserved verbatim
- [ ] Parser raises a named exception (not a crash) on malformed/truncated JSONL
- [ ] `git_branch` is `None` (not a crash) when working directory is not a git repo

---

### C1-2 — Re-prompt Detection + Bail-out Flag
**Complexity: M** | **Depends on: C1-1**

**Artifacts:**
- `collector/reprompt.py` — takes per-turn message list from C1-1; identifies re-prompt sequences (3+ consecutive human turns with equivalent intent); sets `bail_out` flag when `rephrase_count >= threshold`
- `tests/fixtures/reprompt_session.jsonl` — fixture with known re-prompt sequence
- `tests/test_reprompt.py`

**Acceptance criteria:**
- [ ] `rephrase_count: 2` for reprompt fixture; `rephrase_count: 0` for clean fixture
- [ ] `bail_out: true` fires correctly when `rephrase_count >= 3` (constant in config)
- [ ] No external API calls; runs fully offline

---

### C1-3 — SQLite Upsert + Health Write
**Complexity: S** | **Depends on: C1-2, I-1, I-2**

**Artifacts:**
- `collector/store.py` — idempotent upsert of `SessionRecord` into `sessions.db`; calls `write_health("session_parser", count)` on success
- `session_parser.py` updated to wire `parse → reprompt → store` into a single `process_session(filepath)` call
- `tests/test_store.py` — uses `:memory:` SQLite; asserts no duplicate on second upsert of same `session_uuid`

Schema is deferred to Phase 2. For now, the session record stores raw content and identity fields sufficient to locate and link the session. Exact column list TBD.

**Acceptance criteria:**
- [ ] Running `process_session(fixture)` twice produces exactly one row
- [ ] `health.json` contains valid ISO timestamp under `session_parser.last_success`
- [ ] `python -m collector.session_parser <path>` exits 0 and prints `session_uuid`

---

### C1-4 — Hook Mode + Batch Backfill Mode
**Complexity: M** | **Depends on: C1-3**

**Artifacts:**
- `session_parser.py` extended with CLI flags:
  - `--mode hook --session-file <path>` — called by Claude Code `SessionEnd` hook; exits non-zero on failure
  - `--mode batch` — enumerates all JSONL files under configured session path; skips UUIDs already in DB before parsing
- Hook registration snippet added to `setup.md`
- `tests/test_modes.py` — batch mode file discovery and skip logic using a temp directory

**Acceptance criteria:**
- [ ] Hook mode: second call on same fixture exits 0 with no duplicate row
- [ ] Batch mode: 3 files in dir, 1 already in DB → processes 2, DB has 3 rows
- [ ] Batch mode on empty directory exits 0 without error
- [ ] `health.json` updated after both modes

---

### C1-5 — E2E Smoke Test
**Complexity: S** | **Depends on: C1-4**

**Artifacts:**
- `tests/test_e2e.py` — runs `--mode batch` against real `~/.claude/projects/`; skipped in CI via env var guard
- `setup.md` finalized
- Version pin comment in `session_parser.py` noting the ccusage commit the parsing was adapted from

**Acceptance criteria:**
- [ ] Smoke test passes against at least one real session on developer's machine
- [ ] Re-running batch mode produces zero new rows
- [ ] Epic acceptance criterion met: "turn count, tool failure count, and re-prompt count correct for known test session"

---

## Track C2: GitHub Poller

> Depends on I-1, I-2. Prior art: `gh` CLI, github/issue-metrics.

### C2-1 — Raw Data Fetch Layer
**Complexity: M**

**Artifacts:**
- `collector/github_poller/gh_client.py` — thin `gh` subprocess wrapper; handles pagination, rate-limit backoff, JSON deserialization; raises typed exception on non-zero exit
- `collector/github_poller/fetch_issues.py` — wraps `gh issue list` + `gh issue view` + `gh api` for comments; returns normalized dicts: `number`, `title`, `type_label`, `created_at`, `closed_at`, `state`, `body`, `comments` (list of `{author, body, created_at}`)
- `collector/github_poller/fetch_prs.py` — wraps `gh pr list` + `gh pr view` + `gh api` for review comments; returns: `number`, `created_at`, `merged_at`, `review_comment_count`, `head_ref`, `body`, `review_comments` (list of `{author, body, created_at}`)
- Unit tests using recorded fixture JSON (no live network calls)

**Acceptance criteria:**
- [ ] Given fixture JSON, both fetch modules return correct field types including body and comments list
- [ ] `gh_client` raises typed exception on non-zero exit code
- [ ] All tests pass with zero live network calls

---

### C2-2 — PR→Issue Linkage Resolver
**Complexity: S** | **Depends on: C2-1 (field contracts)**

**Artifacts:**
- `collector/github_poller/link_resolver.py` — strategy 1: branch name regex (`feature/123-slug`, `fix/123`); strategy 2: `closes #N` / `fixes #N` body scan; returns `Optional[int]`; strategy 1 first
- Unit tests covering: branch match, body match, both present (branch wins), neither (`None`)

**Acceptance criteria:**
- [ ] All documented resolution cases return correct issue number or `None`
- [ ] No live data or DB dependency

---

### C2-3 — Push-Count Derivation
**Complexity: M** | **Depends on: C2-1**

**Artifacts:**
- `collector/github_poller/push_counter.py` — queries PR commits and reviews via `gh api`; counts commits pushed after first review event timestamp; returns `push_count: int` (0 if no reviews)
- Fixture tests: no reviews → 0; commits before first review → 0; mixed → correct delta

**Acceptance criteria:**
- [ ] `push_counter.count(owner, repo, pr_number)` returns correct integer for each fixture
- [ ] Uses `gh_client` from C2-1; no new subprocess logic

---

### C2-4 — Persistence + Cursor Logic
**Complexity: M** | **Depends on: C2-1, C2-2, C2-3, I-1**

**Artifacts:**
- `collector/github_poller/store.py` — idempotent upsert for issues and PRs; `INSERT OR REPLACE` / `ON CONFLICT DO UPDATE`
- `collector/github_poller/cursor.py` — reads `last_polled_at` per repo; 90-day backfill on first run; `updated:>last_polled_at` delta on subsequent runs; writes cursor after successful batch
- Integration tests: idempotency, cursor backfill vs. delta selection

**Acceptance criteria:**
- [ ] Two identical upserts produce one row
- [ ] Cursor selects correct date window (backfill vs. delta)
- [ ] Missing required field raises, not silently inserts NULL

---

### C2-5 — Orchestrator + Session Linkage + Health Write
**Complexity: L** | **Depends on: C2-4, I-2**

**Artifacts:**
- `collector/github_poller/run.py` — entry point; for each repo: reads cursor → fetches → resolves links → derives push counts → upserts → advances cursor; writes `last_success` to `health.json` only after all repos succeed
- `collector/github_poller/session_linker.py` — after PR upsert, queries `sessions.db` for sessions where `git_branch = pr.head_ref` AND `working_directory` matches repo root; inserts matched pairs into `pr_sessions`; idempotent (`INSERT OR IGNORE`)
- `--dry-run` flag: fetch and parse, skip writes
- E2E integration test against one real repo

**Note:** Session linkage requires `sessions.db` to exist and be populated (C1 track). In environments where C1 has not run, `session_linker.py` exits 0 with zero rows inserted. It does not block the poller.

**Acceptance criteria:**
- [ ] Full run produces correct row counts (verifiable against GitHub UI)
- [ ] Re-run produces identical row count
- [ ] `pr_sessions` contains correct entries for PRs whose `head_ref` matches a known session's `git_branch`
- [ ] Running session linkage twice produces no duplicate rows
- [ ] Session linker exits 0 with zero insertions when `sessions.db` is absent
- [ ] `health.json` updated only when all repos succeed; prior `last_success` preserved on failure
- [ ] `--dry-run` exits 0 with no DB writes

---

## Track C3: App-Switch Logger

> Depends on I-1, I-2. Prior art: ActivityWatch (runtime dependency — install first).

### C3-1 — ActivityWatch Bridge Script
**Complexity: S**

**Artifacts:**
- `collector/appswitch/setup.ps1` — one-time: creates ActivityWatch bucket via REST API if absent
- `collector/appswitch/bridge.ps1` — polls foreground window title + process name every 30s; POSTs heartbeat to ActivityWatch local REST API; logs POST failures to stderr without crashing

**Acceptance criteria:**
- [ ] ActivityWatch Web UI shows window-title events at ~30s cadence while bridge is running
- [ ] Switching apps produces distinct events visible within one polling interval
- [ ] Bridge restarts cleanly after ActivityWatch is stopped and restarted
- [ ] POST failures (AW down) are logged, loop continues

---

### C3-2 — Nightly Export Script
**Complexity: M** | **Depends on: C3-1, I-1, I-2**

**Artifacts:**
- `collector/appswitch/export.py` — queries ActivityWatch REST API for prior day's events; deduplicates on `(timestamp_bucket, window_hash)`; upserts into `appswitch.db`; writes `health.json`
- `tests/fixtures/mock_aw_response.json` — static sample for unit-testing dedup logic

**Deduplication key:** `timestamp_bucket = unix_ts // 30 * 30`, `window_hash = sha256(app + title)[:8]`

**Acceptance criteria:**
- [ ] Export against live AW instance produces rows in `appswitch.db`
- [ ] Running export twice on same data produces identical row count
- [ ] Two events with identical key collapse to one row
- [ ] `health.json` `last_success` updated on success; prior value preserved on failure

---

### C3-3 — Scheduler + Process Supervision
**Complexity: S** | **Depends on: C3-2**

**Artifacts:**
- `collector/appswitch/install_task.ps1` — registers bridge.ps1 (at-logon, indefinite) and export.py (daily 02:00) as Task Scheduler tasks
- `collector/appswitch/uninstall_task.ps1` — removes both tasks cleanly

**Acceptance criteria:**
- [ ] After install, both tasks appear in Task Scheduler with correct triggers
- [ ] After reboot, bridge.ps1 starts automatically; events resume in AW UI
- [ ] After manually triggering nightly task, `appswitch.db` updates and `health.json` reflects new `last_success`
- [ ] Uninstall removes both tasks; bridge does not restart after reboot

---

### C3-4 — Integration Smoke Test
**Complexity: M** | **Depends on: C3-3**

**Artifacts:**
- `collector/appswitch/test/smoke_test.ps1` — end-to-end: confirms AW running, switches windows programmatically, waits 35s, asserts events in AW API, triggers export.py, asserts rows in `appswitch.db`, asserts `health.json` current

**Acceptance criteria:**
- [ ] `smoke_test.ps1` exits 0 on a machine with AW running and tasks installed
- [ ] Exits non-zero with clear failure message when any component is broken
- [ ] Dedup unit test passes against fixture without live AW

---

## Integration Gate

**All three collector tracks complete + I-3 done → run `run_collectors.ps1` end-to-end**

**Acceptance criteria:**
- [ ] `run_collectors.ps1` runs all three collectors sequentially without error
- [ ] Dated log file written under `logs/`
- [ ] `health_check.py` exits 0 after a clean run
- [ ] All three DBs have data
- [ ] Re-running the whole script the following day produces zero duplicate rows in any DB
- [ ] Epic acceptance criteria from `epic-data-collection.md` are all met

---

## Complexity Summary

| Track | Stages | S | M | L | Total |
|-------|--------|---|---|---|-------|
| Infrastructure | 3 | 2 | 1 | 0 | — |
| C1 Session Parser | 5 | 3 | 2 | 0 | — |
| C2 GitHub Poller | 5 | 1 | 3 | 1 | — |
| C3 App-Switch | 4 | 2 | 2 | 0 | — |
| **Total** | **17** | **8** | **8** | **1** | — |

## Critical Path

```
I-1 → I-2 → C2-1 → C2-2 → C2-3 → C2-4 → C2-5 → Integration
```

C2 is the longest and contains the only L-complexity stage. It gates nothing else, but it is the last collector to finish on any realistic schedule. Parallelize C1 and C3 against it.
