# am-i-shipping

A workflow monitor that collects data from your Claude Code sessions, GitHub activity, and application usage patterns. It runs weekly synthesis to identify gaps between what you expected Claude to deliver and what it actually delivered, then proposes experiments to close those gaps. The unit of improvement is your behavior — your setup, your prompts, your plans, your review habits — not Claude's.

This is **not** a dashboard. It is a feedback loop: collect signals, find patterns, change one precondition, measure the result.

---

## Collectors

| # | Collector | What it captures | Data store |
|---|-----------|-----------------|------------|
| C1 | **Session Parser** | Turn counts, tool failures, re-prompt counts, session duration from Claude Code JSONL transcripts | `data/sessions.db` |
| C2 | **GitHub Poller** | Issues, PRs, push counts, issue-PR linkage for configured repos | `data/github.db` |
| C3 | **App-Switch Logger** | Window-focus events from ActivityWatch — context switching patterns during coding sessions | `data/appswitch.db` |

---

## Prerequisites

- **Python 3.11+**
- **`gh` CLI** — installed and authenticated (`gh auth login`). Required for the GitHub poller.
- **ActivityWatch** — installed and running (only required for C3, the app-switch logger). Optional if you only use C1 and C2.

---

## Quick Start

1. **Clone and install:**

   ```bash
   git clone https://github.com/hyang0129/am-i-shipping.git
   cd am-i-shipping
   pip install -e .
   ```

2. **Create your config:**

   ```bash
   cp config.yaml.example config.yaml
   ```

   Edit `config.yaml` and set the required fields:

   ```yaml
   session:
     projects_path: "/Users/<you>/.claude/projects"   # path to Claude Code projects

   github:
     repos:
       - "your-org/your-repo"
   ```

3. **Initialize databases:**

   ```bash
   python -m am_i_shipping.db
   ```

4. **Register the Claude Code hook** (see [setup.md](setup.md) Step 1) so sessions are parsed automatically after each Claude Code session.

5. **Install the scheduled service** so collectors run daily:

   ```bash
   # Linux
   bash scripts/install-cron.sh

   # macOS
   bash scripts/install-launchd.sh

   # Windows (PowerShell)
   .\scripts\install-task.ps1
   ```

   See [setup.md](setup.md) Step 2 for manual alternatives and details.

---

## Manual Run

Run all collectors once:

```bash
# Linux / macOS
bash run_collectors.sh

# Windows
.\run_collectors.ps1
```

---

## Verification

After a collector run, verify everything is healthy:

```bash
python -m am_i_shipping.health_check
```

Exit code 0 means all collectors reported recently. Exit code 1 means one or more collectors are stale or have never run — check the output for details.

---

## Synthesis

Once collectors have been running for at least a week, the synthesis layer
turns the accumulated data into a weekly retrospective. It aggregates your
workflow units, runs cross-unit outlier / abandonment detection, then asks
Claude to produce a Markdown retrospective framed around the idealized
workflow's preconditions — surfacing where you were slow and where effort
was wasted, and asking at most two clarifying questions. It does **not**
prescribe recommendations; that is a separate layer.

### What it does

- Loads the week's `units` rows + graph components from `data/github.db`
  and `data/sessions.db`
- Apportions a 512 KB transcript budget across the week's sessions (see
  `synthesis/weekly.py::water_fill_truncate`)
- Calls the Anthropic API (or an offline fake when `AMIS_SYNTHESIS_LIVE`
  is unset) to render the retrospective
- Writes `retrospectives/YYYY-MM-DD.md` atomically; refuses to overwrite
  an existing file so your hand-written answers under "Clarifying
  Questions" survive a re-run

### How to run

```bash
am-synthesize --week YYYY-MM-DD
```

The `YYYY-MM-DD` anchor must match a value in `units.week_start` (the
graph builder picks a Sunday for each week). Re-running with the same
`--week` is a cheap no-op (the output writer refuses to overwrite).

Dry-run to inspect the assembled prompt without calling the API:

```bash
am-synthesize --week YYYY-MM-DD --dry-run
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `AMIS_SYNTHESIS_LIVE=1` | Use the real Anthropic API instead of the offline fake client. Requires `ANTHROPIC_API_KEY` (or whichever name `config.synthesis.anthropic_api_key_env` resolves to). |
| `AMIS_FORCE_SYNTHESIS=1` | Run `am-synthesize` inside `run_collectors.sh` / `.ps1` even when today is not Sunday. Useful for ad-hoc re-runs or catching up after a missed Sunday. |

### Where output goes

```
retrospectives/YYYY-MM-DD.md      # committed retrospective
retrospectives/.dry-run/*.prompt.txt   # --dry-run artefacts (ignored)
```

### Cadence

`run_collectors.sh` / `run_collectors.ps1` invoke `am-synthesize`
automatically on Sundays (or when `AMIS_FORCE_SYNTHESIS=1`). Daily
collectors still run unconditionally — only the synthesis step is gated
on weekly cadence. See [setup.md](setup.md) for the weekly schedule
step.

---

## Project Structure

```
am-i-shipping/
├── am_i_shipping/           # Core package: config, DB schema, health check
│   ├── config_loader.py     # Loads and validates config.yaml
│   ├── db.py                # SQLite schema initialization
│   ├── health_check.py      # Verifies collector freshness
│   └── health_writer.py     # Writes health.json after each collector run
├── collector/               # Data collectors
│   ├── session_parser.py    # C1: Claude Code session parser
│   ├── store.py             # Session storage layer
│   ├── reprompt.py          # Re-prompt detection logic
│   ├── github_poller/       # C2: GitHub issue/PR poller
│   └── appswitch/           # C3: ActivityWatch app-switch logger
├── scripts/                 # Service install/uninstall scripts
│   ├── install-cron.sh      # Linux: register crontab entry
│   ├── uninstall-cron.sh    # Linux: remove crontab entry
│   ├── install-launchd.sh   # macOS: register launchd agent
│   ├── uninstall-launchd.sh # macOS: remove launchd agent
│   ├── install-task.ps1     # Windows: register Task Scheduler task
│   └── uninstall-task.ps1   # Windows: remove Task Scheduler task
├── tests/                   # Test suite
├── run_collectors.sh        # Entry point (Linux/macOS)
├── run_collectors.ps1       # Entry point (Windows)
├── config.yaml.example      # Example configuration
├── setup.md                 # Detailed setup guide
└── data/                    # Runtime data (gitignored)
    ├── sessions.db
    ├── github.db
    ├── appswitch.db
    └── health.json
```

---

## Reprocessing after #66

If you upgraded from before issue #66, the `units` table definition changed:
session-only connected components (sessions with no linked issue or PR) are no
longer materialised as units. Expect your `units` row count to **drop** after
reprocessing — this is correct, not data loss.

Run these three steps in order:

1. **Create the new table:**
   ```
   python -m am_i_shipping.db
   ```
   This adds the `session_gh_events` table to `data/github.db`.

2. **Re-extract GH events from historical sessions:**
   ```
   python -m collector.session_parser --mode batch
   ```
   Existing sessions are skipped by UUID, so only new sessions are parsed.
   To force a full backfill, delete `data/sessions.db` first (this re-parses
   all transcripts — safe but slow).

3. **Re-run synthesis:**
   ```
   am-synthesize
   ```
   Or: `python -m synthesis.cli`. The graph and units tables are rebuilt under
   the new definition.

---

## Further Reading

- [setup.md](setup.md) — Detailed platform-specific setup instructions, hook configuration, and troubleshooting
- [idealized-workflow.md](idealized-workflow.md) — The workflow model that frames all analysis
- [config.yaml.example](config.yaml.example) — Full configuration reference with comments
