# Setup Guide

Three steps to get the workflow monitor collecting data.

---

## Step 1 — Register Claude Code Hooks

Add a `SessionEnd` hook that triggers the session parser after every Claude Code session.

1. Open your Claude Code settings (or create `~/.claude/settings.json` if it does not exist).
2. Add the following hook configuration:

```json
{
  "hooks": {
    "SessionEnd": [
      {
        "type": "command",
        "command": "python \"<REPO_ROOT>/collector/session_parser.py\" --mode hook --session-file \"$SESSION_FILE\""
      }
    ]
  }
}
```

Replace `<REPO_ROOT>` with the absolute path to this repository (e.g., `C:\Users\you\repos\am-i-shipping`).

**Verify:** Start and end a short Claude Code session. Check `data/health.json` — the `session_parser` entry should have a recent `last_success` timestamp.

---

## Step 2 — Add Task Scheduler Entry

Create a Windows Task Scheduler task that runs the collector pipeline nightly.

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

## Step 3 — Create config.yaml

Copy the example config and fill in the required fields:

```bash
cp config.yaml.example config.yaml
```

Open `config.yaml` and set the required values:

```yaml
session:
  # REQUIRED — path to your Claude Code projects directory
  projects_path: "C:/Users/<you>/.claude/projects"

github:
  # REQUIRED — repos to poll (owner/repo format)
  repos:
    - "your-org/your-repo"
    - "your-org/another-repo"
```

All other fields have sensible defaults. See the comments in `config.yaml.example` for optional settings.

**Verify:** Run `python config_loader.py` (or `python -c "from config_loader import load_config; print(load_config())"`) — it should print the config object without errors.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `config_loader.py` raises `ConfigError` | Check that `session.projects_path` and `github.repos` are set in `config.yaml` |
| `health_check.py` exits 1 | Run `python health_check.py` — it prints which collector is stale or missing |
| No log files in `logs/` | Make sure `run_collectors.ps1` has the correct `Start in` directory in Task Scheduler |
| Session parser not triggering | Verify the hook is registered: check `~/.claude/settings.json` for the `SessionEnd` entry |
