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

## Further Reading

- [setup.md](setup.md) — Detailed platform-specific setup instructions, hook configuration, and troubleshooting
- [idealized-workflow.md](idealized-workflow.md) — The workflow model that frames all analysis
- [config.yaml.example](config.yaml.example) — Full configuration reference with comments
