# Epic: Passive Data Collection Layer

*Created: 2026-04-14*
*Phase: 1 of 3 (Passive Collection — weeks 1–3)*

---

## Goal

Deploy three independent, always-on collectors that passively capture developer workflow data with zero manual intervention. Establish a clean, deduplicated, queryable local dataset that becomes the input for the weekly synthesis engine in Phase 2.

No analysis. No dashboards. Collect only.

---

## Background

This epic implements **Phase 1** of the Workflow Monitor system (see `concept.md`). The system monitors a single developer's Claude Code usage, GitHub issue/PR lifecycle, and app-switching behavior to surface friction and velocity patterns over time.

The collection layer must be:
- **Fully passive** — no tagging, no manual triggers
- **Idempotent** — re-running any collector over historical data produces identical records
- **Independently deployable** — one collector failing does not affect the others
- **Observable** — silent failures are worse than noisy ones

---

## Data Sources and Collectors

### Collector 1: Claude Code Session Parser

**Source:** `~/.claude/projects/<hash>/sessions/<uuid>.jsonl`
**Trigger:** Post-session Claude Code hook (`SessionEnd`) + nightly batch backfill
**Prior art:** ccusage (parsing), disler/hooks-observability (hook integration)

**Signals to extract per session:**
- Turn count
- Tool call count and failure count
- Re-prompt count (same intent restated 3+ turns — requires turn-level analysis)
- Session duration
- Working directory (for session → issue correlation join)
- Bail-out flag (high rephrase count + zero committed lines)

**Storage:** `data/sessions.db` (SQLite), primary key: `session_uuid`

---

### Collector 2: GitHub Poller

**Source:** GitHub REST API via `gh` CLI
**Trigger:** Nightly scheduled run (Task Scheduler)
**Prior art:** github/issue-metrics, simonw/claude-code-transcripts

**Data to collect:**
- Issues: number, title, type label, created_at, closed_at, state
- PRs: number, linked issue, created_at, merged_at, review_comment_count, push_count (review cycles)
- PR-to-issue linkage (via branch name or `closes #N` references)

**Cursor strategy:** Store `last_polled_at` per repo. First run: 90-day backfill. Subsequent runs: `updated:>last_polled_at` filter.

**Storage:** `data/github.db` (SQLite), primary keys: `(repo, issue_number)` and `(repo, pr_number)`

---

### Collector 3: App-Switch Logger

**Source:** Windows foreground window events
**Trigger:** Always-on background process (ActivityWatch + PowerShell POST bridge)
**Prior art:** ActivityWatch (cross-platform, local REST API, crash-resilient)

**Data to collect:**
- Window title + application name
- Timestamp (floored to 30s bucket)
- Duration in window

**Deduplication key:** `(timestamp_bucket, window_hash)`

**Storage:** ActivityWatch local DB → nightly export to `data/appswitch.db`

---

## Storage Architecture

- **Primary store:** SQLite per data type (`sessions.db`, `github.db`, `appswitch.db`)
- **Synthesis export:** JSONL files (`data/sessions.jsonl`, `data/github.jsonl`, `data/appswitch.jsonl`) — written on-demand by synthesis engine, not by collectors
- **Health tracking:** `data/health.json` — each collector writes `last_success` timestamp after every successful run
- **Config:** `config.yaml` — repo list, GitHub token ref, session path, ActivityWatch endpoint

---

## Acceptance Criteria

- [ ] All three collectors run without errors on a clean machine given only `config.yaml`
- [ ] Re-running any collector over the same historical data produces zero new records (idempotency verified)
- [ ] `data/health.json` is updated after every collector run; stale check warns if >48h
- [ ] Session parser correctly extracts turn count, tool failure count, and re-prompt count for a known test session
- [ ] GitHub poller correctly handles the cursor — no duplicate records after two consecutive runs
- [ ] App-switch data flows from ActivityWatch → `appswitch.db` with correct deduplication
- [ ] A single `run_collectors.ps1` entry point runs all three collectors in sequence
- [ ] Setup requires exactly three user actions: hook registration, Task Scheduler entry, `config.yaml` edit

---

## Delivery Stages

Full breakdown: [deliverables-data-collection.md](deliverables-data-collection.md)

Four parallel tracks after a shared infrastructure gate. Infrastructure must reach **I-2** before any collector work begins.

```
INFRA ──► I-1 ──► I-2 ──► I-3 (entry point + docs, non-blocking)
                  │
          ┌───────┼────────────┐
          ▼       ▼            ▼
       C1 track  C2 track    C3 track
       (5 stages)(5 stages)  (4 stages)
          └───────┴────────────┘
                  │
           Integration gate
```

| Stage | Description | Complexity |
|-------|-------------|------------|
| **I-1** | DB schema + config loader | M |
| **I-2** | Health writer + health check | S |
| **I-3** | `run_collectors.ps1` + `setup.md` | S |
| **C1-1** | Session JSONL parser + fixture harness | S |
| **C1-2** | Re-prompt detection + bail-out flag | M |
| **C1-3** | SQLite upsert + health write | S |
| **C1-4** | Hook mode + batch backfill mode | M |
| **C1-5** | E2E smoke test against live data | S |
| **C2-1** | GitHub raw data fetch layer | M |
| **C2-2** | PR→issue linkage resolver | S |
| **C2-3** | Push-count derivation | M |
| **C2-4** | Persistence + cursor logic | M |
| **C2-5** | Orchestrator + health write | L |
| **C3-1** | ActivityWatch bridge script | S |
| **C3-2** | Nightly export to SQLite | M |
| **C3-3** | Task Scheduler + process supervision | S |
| **C3-4** | Integration smoke test | M |

**Critical path:** I-1 → I-2 → C2-1 → C2-2 → C2-3 → C2-4 → C2-5 → Integration gate

C2 is the longest track (only L-complexity stage is C2-5). Run C1 and C3 in parallel against it.

---

## Out of Scope (Phase 1)

- Data analysis or synthesis of any kind
- Weekly report generation
- Experiment loop
- Any UI or dashboard
- Cloud sync (config option reserved but not implemented)

---

## Repo Structure

```
workflow-monitor/
  collector/
    session_parser.py        # ccusage-derived; hook + batch modes (C1)
    reprompt.py              # re-prompt detection algorithm (C1)
    store.py                 # SQLite upsert utilities (C1)
    github_poller/
      gh_client.py           # gh subprocess wrapper (C2)
      fetch_issues.py        # (C2)
      fetch_prs.py           # (C2)
      link_resolver.py       # PR→issue linkage (C2)
      push_counter.py        # review cycle counting (C2)
      store.py               # issue/PR upserts + cursor (C2)
      run.py                 # orchestrator entry point (C2)
    appswitch/
      setup.ps1              # one-time AW bucket creation (C3)
      bridge.ps1             # 30s polling → AW REST API (C3)
      export.py              # nightly AW → appswitch.db (C3)
      install_task.ps1       # Task Scheduler registration (C3)
      uninstall_task.ps1     # (C3)
      test/
        smoke_test.ps1
        fixtures/
  data/
    sessions.db
    github.db
    appswitch.db
    health.json
  tests/
    fixtures/
      sample_session.jsonl
      reprompt_session.jsonl
    test_parser.py
    test_reprompt.py
    test_store.py
    test_modes.py
    test_e2e.py
  logs/                      # dated run logs from run_collectors.ps1
  init_db.py                 # idempotent schema init (I-1)
  config_loader.py           # config.yaml validation (I-1)
  health_writer.py           # shared health.json writer (I-2)
  health_check.py            # stale collector check (I-2)
  run_collectors.ps1         # single entry point (I-3)
  config.yaml
  setup.md                   # three-step setup instructions
```

---

## Dependencies

| Dependency | Type | Required for |
|---|---|---|
| ccusage (parsing logic) | Reference/fork | session_parser.py |
| disler/hooks-observability | Reference | Hook schema + SQLite pattern |
| ActivityWatch | Runtime install | appswitch_bridge.ps1 |
| `gh` CLI | Runtime | github_poller.py |
| Python 3.11+ | Runtime | All Python collectors |
| Task Scheduler (Windows) | Runtime | Nightly runs |

---

## Risks

| Risk | Mitigation |
|---|---|
| ccusage session format changes | Pin to a known commit; test against a fixture file |
| ActivityWatch not running on startup | Health check catches it within 48h |
| GitHub rate limits | Cursor strategy limits requests to delta only |
| Session boundary ambiguity | 30-min gap rule encoded in config as a constant |
