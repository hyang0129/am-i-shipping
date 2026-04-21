"""Backfill ``session_gh_events`` for all sessions in sessions.db.

``session_gh_events`` is populated during live session parsing by scanning
tool_use / tool_result blocks in the raw JSONL files. However, batch mode
skips sessions already in the DB, so existing rows never got their gh_events
populated. This script re-reads each JSONL file, extracts gh_events, and
inserts them into github.db::session_gh_events.

``raw_content_json`` strips tool blocks by design, so the JSONL file is the
only source of truth for gh CLI and git push commands.

Invocation::

    python -m am_i_shipping.scripts.backfill_gh_events \\
        [--config path/to/config.yaml] [--dry-run] [--limit N]

``--dry-run`` skips inserts and prints counts.
``--limit`` caps the number of JSONL files processed per invocation.

Safe to re-run: uses INSERT OR IGNORE keyed on (session_uuid, event_type,
repo, ref), so duplicate runs are no-ops for already-inserted events.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _extract_gh_events_from_file(filepath: Path) -> tuple[Optional[str], list]:
    """Read a JSONL session file and return (session_uuid, gh_events)."""
    session_uuid: Optional[str] = None
    gh_events_list: list = []
    pending_creates: Dict[str, Any] = {}

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if session_uuid is None and "sessionId" in entry:
                    session_uuid = entry["sessionId"]

                gh_events_list.extend(_extract_gh_events(entry, pending_creates))

    except OSError as exc:
        print(f"WARN: cannot read {filepath}: {exc}", file=sys.stderr)
        return None, []

    # Flush unresolved pending creates
    gh_events_list.extend(pending_creates.values())

    # Deduplicate by (event_type, repo, ref); keep all pending refs
    seen: set = set()
    deduped: list = []
    for ev in gh_events_list:
        if ev["ref"] == "pending":
            deduped.append(ev)
            continue
        key = (ev["event_type"], ev["repo"], ev["ref"])
        if key not in seen:
            seen.add(key)
            deduped.append(ev)

    return session_uuid, deduped


def _extract_gh_events(entry: dict, pending_creates: dict) -> list:
    """Extract gh CLI and git push events from a single JSONL entry.

    Copied from collector.session_parser to avoid import-time side effects
    and allow this script to run standalone.
    """
    if entry.get("type") not in ("user", "assistant"):
        return []
    content = entry.get("message", {}).get("content", "")
    if not isinstance(content, list):
        return []

    created_at = entry.get("timestamp", "")
    events: list = []

    for block in content:
        if not isinstance(block, dict):
            continue
        btype = block.get("type", "")

        if btype == "tool_use" and block.get("name") == "Bash":
            command = block.get("input", {}).get("command", "") or ""
            tool_use_id = block.get("id", "")
            ev = None

            m = re.search(r"gh\s+issue\s+create\b.*?--repo\s+(\S+)", command, re.DOTALL)
            if m:
                ev = {"event_type": "issue_create", "repo": m.group(1), "ref": "pending",
                      "url": "", "confidence": "medium", "created_at": created_at}

            if ev is None:
                m = re.search(r"gh\s+issue\s+create\b", command, re.DOTALL)
                if m:
                    ev = {"event_type": "issue_create", "repo": "", "ref": "pending",
                          "url": "", "confidence": "medium", "created_at": created_at}

            if ev is None:
                m = re.search(r"gh\s+issue\s+comment\s+(\d+)\s+.*?--repo\s+(\S+)", command, re.DOTALL)
                if m:
                    ev = {"event_type": "issue_comment", "repo": m.group(2), "ref": m.group(1),
                          "url": "", "confidence": "high", "created_at": created_at}
            if ev is None:
                m = re.search(r"gh\s+issue\s+comment\s+--repo\s+(\S+)\s+(\d+)", command, re.DOTALL)
                if m:
                    ev = {"event_type": "issue_comment", "repo": m.group(1), "ref": m.group(2),
                          "url": "", "confidence": "high", "created_at": created_at}

            if ev is None:
                m = re.search(r"gh\s+pr\s+create\b.*?--repo\s+(\S+)", command, re.DOTALL)
                if m:
                    ev = {"event_type": "pr_create", "repo": m.group(1), "ref": "pending",
                          "url": "", "confidence": "medium", "created_at": created_at}

            if ev is None:
                m = re.search(r"gh\s+pr\s+create\b", command, re.DOTALL)
                if m:
                    ev = {"event_type": "pr_create", "repo": "", "ref": "pending",
                          "url": "", "confidence": "medium", "created_at": created_at}

            if ev is None:
                m = re.search(r"gh\s+pr\s+comment\s+(\d+)\s+.*?--repo\s+(\S+)", command, re.DOTALL)
                if m:
                    ev = {"event_type": "pr_comment", "repo": m.group(2), "ref": m.group(1),
                          "url": "", "confidence": "high", "created_at": created_at}
            if ev is None:
                m = re.search(r"gh\s+pr\s+comment\s+--repo\s+(\S+)\s+(\d+)", command, re.DOTALL)
                if m:
                    ev = {"event_type": "pr_comment", "repo": m.group(1), "ref": m.group(2),
                          "url": "", "confidence": "high", "created_at": created_at}

            if ev is None:
                m = re.search(
                    r"gh\s+issue\s+comment\s+https://github\.com/([\w.\-]+/[\w.\-]+)/issues/(\d+)",
                    command, re.DOTALL)
                if m:
                    ev = {"event_type": "issue_comment", "repo": m.group(1), "ref": m.group(2),
                          "url": f"https://github.com/{m.group(1)}/issues/{m.group(2)}",
                          "confidence": "high", "created_at": created_at}

            if ev is None:
                m = re.search(
                    r"gh\s+pr\s+comment\s+https://github\.com/([\w.\-]+/[\w.\-]+)/pull/(\d+)",
                    command, re.DOTALL)
                if m:
                    ev = {"event_type": "pr_comment", "repo": m.group(1), "ref": m.group(2),
                          "url": f"https://github.com/{m.group(1)}/pull/{m.group(2)}",
                          "confidence": "high", "created_at": created_at}

            if ev is None and re.search(r"\bgit\s+push\b", command, re.DOTALL):
                branch_ref = ""
                tokens = command.split()
                try:
                    push_idx = next(i for i, t in enumerate(tokens) if t == "push")
                except StopIteration:
                    push_idx = None
                if push_idx is not None:
                    rest = [t for t in tokens[push_idx + 1:] if not t.startswith("-")]
                    if len(rest) >= 2:
                        raw = rest[1]
                        if ":" in raw:
                            raw = raw.split(":")[-1]
                        if raw.startswith("refs/heads/"):
                            raw = raw[len("refs/heads/"):]
                        branch_ref = raw
                ev = {"event_type": "git_push", "repo": "", "ref": branch_ref,
                      "url": "", "confidence": "medium", "created_at": created_at}

            if ev is not None:
                if ev["ref"] == "pending" and tool_use_id:
                    pending_creates[tool_use_id] = ev
                else:
                    events.append(ev)

        elif btype == "tool_result":
            tool_use_id = block.get("tool_use_id", "")
            if tool_use_id and tool_use_id in pending_creates:
                ev = pending_creates.pop(tool_use_id)
                result_content = block.get("content", "")
                if isinstance(result_content, list):
                    result_text = " ".join(
                        c.get("text", "") if isinstance(c, dict) else str(c)
                        for c in result_content
                    )
                else:
                    result_text = str(result_content) if result_content else ""

                m = re.search(
                    r"https://github\.com/([\w.\-]+/[\w.\-]+)/issues/(\d+)",
                    result_text, re.DOTALL)
                if m:
                    ev["ref"] = m.group(2)
                    ev["url"] = m.group(0)
                    ev["confidence"] = "high"
                    if not ev["repo"]:
                        ev["repo"] = m.group(1)
                else:
                    m = re.search(
                        r"https://github\.com/([\w.\-]+/[\w.\-]+)/pull/(\d+)",
                        result_text, re.DOTALL)
                    if m:
                        ev["ref"] = m.group(2)
                        ev["url"] = m.group(0)
                        ev["confidence"] = "high"
                        if not ev["repo"]:
                            ev["repo"] = m.group(1)
                events.append(ev)

    return events


def _discover_session_files(projects_path: Path) -> List[Path]:
    files = []
    for jsonl in projects_path.rglob("*.jsonl"):
        if "subagents" in jsonl.parts:
            continue
        files.append(jsonl)
    return sorted(files)


def _get_already_populated(gh_conn: sqlite3.Connection) -> set:
    """Return set of session_uuids that already have rows in session_gh_events."""
    cur = gh_conn.execute("SELECT DISTINCT session_uuid FROM session_gh_events")
    return {row[0] for row in cur.fetchall()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill session_gh_events from JSONL files")
    parser.add_argument("--config", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Max files to process (0=all)")
    args = parser.parse_args()

    from am_i_shipping.config_loader import load_config
    from am_i_shipping.db import init_github_db

    config = load_config(args.config)
    data_dir = config.data_path
    github_db_path = data_dir / "github.db"

    init_github_db(github_db_path)
    gh_conn = sqlite3.connect(str(github_db_path))

    already_done = _get_already_populated(gh_conn)
    print(f"Sessions already in session_gh_events: {len(already_done)}", file=sys.stderr)

    projects_path = Path(config.session.projects_path)
    session_files = _discover_session_files(projects_path)
    print(f"JSONL files found: {len(session_files)}", file=sys.stderr)

    processed = 0
    skipped_existing = 0
    no_uuid = 0
    no_events = 0
    total_events_inserted = 0
    errors = 0

    for sf in session_files:
        if args.limit and processed >= args.limit:
            print(f"  Reached --limit={args.limit}", file=sys.stderr)
            break

        session_uuid, gh_events = _extract_gh_events_from_file(sf)

        if session_uuid is None:
            no_uuid += 1
            continue

        if session_uuid in already_done:
            skipped_existing += 1
            continue

        if not gh_events:
            no_events += 1
            processed += 1
            continue

        if not args.dry_run:
            try:
                for ev in gh_events:
                    gh_conn.execute(
                        "INSERT OR IGNORE INTO session_gh_events "
                        "(session_uuid, event_type, repo, ref, url, confidence, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (session_uuid, ev["event_type"], ev["repo"], ev["ref"],
                         ev.get("url"), ev.get("confidence"), ev.get("created_at")),
                    )
                gh_conn.commit()
                already_done.add(session_uuid)
                total_events_inserted += len(gh_events)
            except sqlite3.Error as exc:
                print(f"WARN: DB error for {session_uuid}: {exc}", file=sys.stderr)
                errors += 1
        else:
            total_events_inserted += len(gh_events)

        processed += 1

        if processed % 500 == 0:
            print(f"  Progress: {processed} processed, {total_events_inserted} events so far...",
                  file=sys.stderr)

    gh_conn.close()

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(
        f"{prefix}Done: {processed} files processed, "
        f"{skipped_existing} skipped (already populated), "
        f"{no_uuid} skipped (no UUID), "
        f"{no_events} had no gh events, "
        f"{total_events_inserted} events inserted, "
        f"{errors} errors",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
