# Setup Guide

Three steps to get the workflow monitor collecting data.

---

## Step 1 — Register Claude Code Hooks

Add a `SessionEnd` hook that triggers the session parser after every Claude Code session.

1. Open your Claude Code settings (or create `~/.claude/settings.json` if it does not exist).
2. Add the following hook configuration, replacing `<REPO_ROOT>` with the absolute path to this repository:

```json
{
  "hooks": {
    "SessionEnd": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 -m collector.session_parser --mode hook --session-file \"$SESSION_FILE\" --config \"<REPO_ROOT>/config.yaml\""
          }
        ]
      }
    ]
  }
}
```

> **Note:** `python3` must resolve to the interpreter for which this package is installed. If your environment uses a different name or path (e.g. a virtualenv), replace `python3` with the full path, for example `/usr/local/bin/python3`.
>
> If the `collector` package is not on the interpreter's path (common with Homebrew Python and editable installs), prefix the command with `PYTHONPATH=<REPO_ROOT>`:
> ```
> PYTHONPATH=<REPO_ROOT> python3 -m collector.session_parser ...
> ```

**Verify:** Start and end a short Claude Code session. Check `data/health.json` — the `session_parser` entry should have a recent `last_success` timestamp.

---

## Step 2 — Schedule Nightly Collection

The easiest way to set up scheduled collection is with the provided install scripts. Each script is idempotent — safe to re-run.

### Quick install (recommended)

```bash
# Linux
bash scripts/install-cron.sh

# macOS
bash scripts/install-launchd.sh

# Windows (PowerShell, run as Administrator)
.\scripts\install-task.ps1
```

To remove the scheduled task later:

```bash
# Linux
bash scripts/uninstall-cron.sh

# macOS
bash scripts/uninstall-launchd.sh

# Windows (PowerShell)
.\scripts\uninstall-task.ps1
```

All install scripts default to daily at 02:00. They also install a boot-time fallback trigger so that if your PC is off at 02:00, the run happens automatically the next time you log in. Collectors are idempotent — running twice in one day produces no duplicate data.

### Weekly synthesis cadence

`run_collectors.sh` and `run_collectors.ps1` contain a weekly-cadence
check that runs `am-synthesize` only on Sundays (or when the
`AMIS_FORCE_SYNTHESIS=1` environment variable is set). **You do not need
a separate cron entry / Scheduled Task for synthesis** — as long as the
daily collector job fires on Sunday, synthesis will run with it.

How it works in each entry point:

- **Linux/macOS (`run_collectors.sh`)** — `date +%u` returns `7` on
  Sunday. When it does, the script resolves the most recent Sunday's
  date and invokes `am-synthesize --week <YYYY-MM-DD>`. On all other
  days the block is skipped.
- **Windows (`run_collectors.ps1`)** — the same logic via
  `(Get-Date).DayOfWeek -eq [DayOfWeek]::Sunday`.

If you miss a Sunday (PC off, cron didn't fire), re-run catch-up
manually:

```bash
# Linux / macOS
AMIS_FORCE_SYNTHESIS=1 bash run_collectors.sh

# Windows
$env:AMIS_FORCE_SYNTHESIS = "1"; .\run_collectors.ps1
```

Or call `am-synthesize` directly with the week you want to re-generate:

```bash
am-synthesize --week 2026-04-12
```

Re-running for the same `--week` is a cheap no-op — the output writer
refuses to overwrite an existing retrospective, so answers you've added
under "Clarifying Questions" are safe.

**Verify:** On the Sunday after install, check that
`retrospectives/<sunday-date>.md` exists and that `data/health.json`
contains a fresh `synthesis` entry. `python -m am_i_shipping.health_check`
will warn if the `synthesis` collector has been silent beyond the weekly
threshold (currently 8 days — see
`am_i_shipping/health_check.py::STALE_THRESHOLDS["synthesis"]` for the
source of truth).

---

### Manual setup (alternative)

If you prefer to configure the service manually instead of using the scripts above:

#### macOS — launchd

1. Create a file at `~/Library/LaunchAgents/com.am-i-shipping.collectors.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.am-i-shipping.collectors</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string><REPO_ROOT>/run_collectors.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string><REPO_ROOT></string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>2</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string><REPO_ROOT>/logs/launchd.out.log</string>
    <key>StandardErrorPath</key>
    <string><REPO_ROOT>/logs/launchd.err.log</string>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
```

2. Load it:

```bash
launchctl load ~/Library/LaunchAgents/com.am-i-shipping.collectors.plist
```

**Verify:** `launchctl list | grep am-i-shipping` should show an entry. To test immediately: `launchctl start com.am-i-shipping.collectors`, then check `logs/` for output.

---

#### Windows — Task Scheduler

1. Open Task Scheduler (`taskschd.msc`).
2. Click **Create Task** (not "Create Basic Task").
3. Configure:

| Tab | Setting | Value |
|-----|---------|-------|
| General | Name | `am-i-shipping-collectors` |
| General | Run whether user is logged on or not | Checked |
| Triggers | Begin the task | On a schedule |
| Triggers | Daily at | `02:00` (or your preferred time) |
| Actions | Program/script | `powershell.exe` |
| Actions | Arguments | `-ExecutionPolicy Bypass -File "<REPO_ROOT>\run_collectors.ps1"` |
| Actions | Start in | `<REPO_ROOT>` |
| Settings | If the task fails, restart every | 5 minutes, up to 3 times |

Replace `<REPO_ROOT>` with the absolute path to this repository.

**Verify:** Right-click the task and choose **Run**. Check `logs/` for a dated log file. Check `data/health.json` for updated timestamps.

---

#### Linux — cron

Add a crontab entry:

```bash
crontab -e
```

```
0 2 * * * cd <REPO_ROOT> && bash run_collectors.sh >> logs/cron.log 2>&1
@reboot sleep 60 && cd <REPO_ROOT> && bash run_collectors.sh >> logs/cron.log 2>&1
```

The `@reboot` line recovers missed runs when the PC was off at 02:00. Both entries are safe to have simultaneously — collectors are idempotent.

---

## Step 3 — Create config.yaml

Copy the example config and fill in the required fields:

```bash
cp config.yaml.example config.yaml
```

Open `config.yaml` and set the required values:

```yaml
session:
  # REQUIRED — path to your Claude Code projects directory
  # macOS/Linux: /Users/<you>/.claude/projects
  # Windows:     C:/Users/<you>/.claude/projects
  projects_path: "/Users/<you>/.claude/projects"

github:
  # REQUIRED — repos to poll (owner/repo format)
  repos:
    - "your-org/your-repo"
    - "your-org/another-repo"
```

All other fields have sensible defaults. See the comments in `config.yaml.example` for optional settings.

**Verify:** Run the following from the repo root — it should print the config without errors:

```bash
PYTHONPATH=. python3 -c "from am_i_shipping.config_loader import load_config; print(load_config())"
```

---

## Step 4 — Backfill Session Timestamps (one-time)

If you already have historical Claude Code sessions recorded in `~/.claude/projects/`
before installing this repo, the first collector run will import them into
`data/sessions.db` but leave `session_started_at` / `session_ended_at` as `NULL` for
every pre-existing row. Unit metrics such as `dark_time_pct` and session-rooted
`elapsed_days` depend on those columns, so until the backfill runs the weekly
synthesis unit-summary table will show zeros.

Run the backfill exactly once per install (and any time you import historical
sessions from a new source):

```bash
python -m am_i_shipping.scripts.backfill_session_timestamps
```

The script walks every row whose timestamps are `NULL`, re-opens the JSONL file
under `session.projects_path` that produced it, and `UPDATE`s only the two
timestamp columns in place — every other column (including `raw_content_json`)
is preserved byte-for-byte. It commits in batches of 500, so interrupting and
re-running is safe. Running it a second time after all rows are populated is a
cheap no-op — already-populated rows are filtered out by the `WHERE` clause.

Useful flags:

- `--dry-run` — print counts of what would be updated without writing.
- `--limit N` — cap the number of rows touched per invocation (smoke-test).
- `--config path/to/config.yaml` — override the default config lookup.

Rows whose JSONL file is no longer on disk are logged as warnings and left with
`NULL` timestamps; this is preferable to silently synthesising data. The
integration-test gate in `tests/test_integration_gate.py` enforces that at least
90% of `sessions` rows have a populated `session_started_at` before weekly
synthesis is allowed to run — if this threshold is not met after a backfill,
check the warnings in stderr to see which JSONL files went missing.

**Verify:**

```bash
python3 -c "import sqlite3; c = sqlite3.connect('data/sessions.db'); \
  t = c.execute('SELECT COUNT(*) FROM sessions').fetchone()[0]; \
  n = c.execute('SELECT COUNT(*) FROM sessions WHERE session_started_at IS NOT NULL').fetchone()[0]; \
  print(f'{n}/{t} rows have timestamps ({100*n/t:.1f}%)')"
```

You should see at least 90% coverage. Historical installs typically reach 97–99%;
the remainder are sessions whose JSONL files were rotated off disk before the
backfill ran.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `ConfigError` on startup | Check that `session.projects_path` and `github.repos` are set in `config.yaml` |
| `health_check.py` exits 1 | Run `PYTHONPATH=. python3 -m am_i_shipping.health_check` — it prints which collector is stale or missing |
| No log files in `logs/` | Make sure `logs/` directory exists (`mkdir -p logs`); on Windows verify `Start in` is set correctly in Task Scheduler |
| Session parser not triggering | Verify the hook is registered in `~/.claude/settings.json`; check that the `SessionEnd` array items have a nested `hooks` array (see Step 1) |
| `ModuleNotFoundError: No module named 'collector'` | The `PYTHONPATH` env var must point to the repo root; set it explicitly in the hook command or launchd plist |
| `am-session-parser: command not found` | Use `python3 -m collector.session_parser` instead — editable installs with Homebrew Python do not always place scripts on `$PATH` |
