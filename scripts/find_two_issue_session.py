"""Find sessions that create 2 issues resolved via 2 separate PRs.

Searches all JSONL project dirs for sessions where:
- Exactly 2 issue_create events (resolved, not pending)
- At least 2 pr_create events (resolved, not pending)

Usage (from repo root with venv active):
    python scripts/find_two_issue_session.py
"""
from __future__ import annotations

import sys
from pathlib import Path

from collector.session_parser import parse_session

CLAUDE_PROJECTS = Path.home() / ".claude" / "projects"
# Auto-discover all project dirs under ~/.claude/projects/ so future
# workspace containers are picked up without editing this list.
PROJECT_DIRS = [
    d for d in (CLAUDE_PROJECTS).iterdir()
    if d.is_dir() and not d.name.startswith(".")
]

candidates = []

for proj_dir in PROJECT_DIRS:
    if not proj_dir.exists():
        continue
    for jsonl_path in sorted(proj_dir.glob("*.jsonl")):
        try:
            record = parse_session(jsonl_path)
        except Exception:
            continue

        gh = record.gh_events or []
        issues = [e for e in gh if e["event_type"] == "issue_create" and e["ref"] != "pending"]
        prs = [e for e in gh if e["event_type"] == "pr_create" and e["ref"] != "pending"]

        if len(issues) == 2 and len(prs) >= 2:
            repos = {e["repo"] for e in issues + prs if e["repo"]}
            candidates.append({
                "path": jsonl_path,
                "session_uuid": record.session_uuid,
                "issues": issues,
                "prs": prs,
                "repos": repos,
                "turn_count": record.turn_count,
                "reprompts": record.reprompt_count,
            })

if not candidates:
    print("No sessions found matching criteria.")
    sys.exit(1)

# Prefer single-repo sessions, then fewest turns
candidates.sort(key=lambda c: (len(c["repos"]), c["turn_count"]))

print(f"Found {len(candidates)} candidate(s):\n")
for c in candidates[:10]:
    print(f"  {c['path'].name}")
    print(f"    uuid:     {c['session_uuid']}")
    print(f"    repos:    {c['repos']}")
    print(f"    turns:    {c['turn_count']}  reprompts: {c['reprompts']}")
    print(f"    issues:   {[(e['repo'], e['ref']) for e in c['issues']]}")
    print(f"    prs:      {[(e['repo'], e['ref']) for e in c['prs']]}")
    print()
