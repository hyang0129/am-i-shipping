"""JSONL session parser for Claude Code sessions.

Parses Claude Code JSONL session logs and extracts structured metrics.
Adapted from ccusage parsing logic (commit reference: conceptual adaptation,
not a direct port — ccusage uses TypeScript).

Usage:
    # Hook mode (called by Claude Code SessionEnd hook):
    python -m collector.session_parser --mode hook --session-file <path>

    # Batch mode (backfill all sessions under projects_path):
    python -m collector.session_parser --mode batch [--config path/to/config.yaml]
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .reprompt import detect_reprompts
from .store import upsert_session


class SessionParseError(Exception):
    """Raised when a JSONL session file is malformed or cannot be parsed."""


@dataclass
class SessionRecord:
    """Parsed session data ready for storage."""

    session_uuid: str
    turn_count: int
    tool_call_count: int
    tool_failure_count: int
    reprompt_count: int
    bail_out: bool
    session_duration_seconds: float
    working_directory: Optional[str]
    git_branch: Optional[str]
    raw_content_json: str
    input_tokens: int
    output_tokens: int
    cache_creation_tokens: int
    cache_read_tokens: int
    fast_mode_turns: int
    # Epic #17 — Sub-Issue 2 (Decision 1): synthesis-layer session anchors.
    # ``session_started_at`` is the first timestamp observed in the JSONL
    # (across all entry types), and ``session_ended_at`` is the last. Both
    # are ISO-8601 strings with tzinfo offset preserved from the source, or
    # ``None`` when no timestamps were parsed (malformed or ancient session).
    session_started_at: Optional[str] = None
    session_ended_at: Optional[str] = None


def _git_branch_from_dir(cwd: str) -> Optional[str]:
    """Resolve the current git branch from a working directory.

    Returns None if the directory doesn't exist or isn't a git repo.
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            branch = result.stdout.strip()
            return branch if branch else None
        return None
    except (subprocess.SubprocessError, OSError):
        return None


def _strip_content_blocks(content: Any) -> Any:
    """Remove thinking, tool_use, and tool_result blocks from message content.

    Preserves user and assistant text turns verbatim.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        filtered = []
        for block in content:
            if isinstance(block, dict):
                block_type = block.get("type", "")
                if block_type in ("thinking", "tool_use", "tool_result"):
                    continue
                filtered.append(block)
            else:
                filtered.append(block)
        return filtered if filtered else None
    return content


def parse_session(filepath: str | Path, threshold: int = 3) -> SessionRecord:
    """Parse a Claude Code JSONL session file into a SessionRecord.

    Parameters
    ----------
    filepath:
        Path to the .jsonl session file.
    threshold:
        Reprompt count at or above which ``bail_out`` is set to True.
        Defaults to 3.

    Returns
    -------
    SessionRecord with all extracted fields.

    Raises
    ------
    SessionParseError
        If the file is empty, not valid JSONL, or missing required fields.
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise SessionParseError(f"Session file not found: {filepath}")

    messages: List[Dict[str, Any]] = []
    session_uuid: Optional[str] = None
    working_directory: Optional[str] = None
    git_branch: Optional[str] = None
    timestamps: List[datetime] = []
    tool_call_count = 0
    tool_failure_count = 0
    turn_count = 0
    raw_content_turns: List[Dict[str, Any]] = []
    input_tokens = 0
    output_tokens = 0
    cache_creation_tokens = 0
    cache_read_tokens = 0
    fast_mode_turns = 0

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            line_num = 0
            for line in f:
                line_num += 1
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise SessionParseError(
                        f"Malformed JSON at line {line_num} in {filepath}: {exc}"
                    ) from exc

                entry_type = entry.get("type")

                # Extract session UUID from any entry that has it
                if session_uuid is None and "sessionId" in entry:
                    session_uuid = entry["sessionId"]

                # Extract timestamp
                ts_str = entry.get("timestamp")
                if ts_str:
                    try:
                        ts = datetime.fromisoformat(
                            ts_str.replace("Z", "+00:00")
                        )
                        timestamps.append(ts)
                    except ValueError:
                        pass

                # Only process user/assistant message entries
                if entry_type not in ("user", "assistant"):
                    continue

                msg = entry.get("message", {})
                role = msg.get("role", entry_type)
                content = msg.get("content", "")

                # Extract working directory from first user message
                if working_directory is None and entry.get("cwd"):
                    working_directory = entry["cwd"]

                # Extract git branch from first user message that has it
                if git_branch is None and entry.get("gitBranch"):
                    git_branch = entry["gitBranch"]

                # Count turns (user messages that are not pure tool results)
                if entry_type == "user":
                    is_tool_result_only = False
                    if isinstance(content, list):
                        non_tool = [
                            b
                            for b in content
                            if isinstance(b, dict)
                            and b.get("type") != "tool_result"
                        ]
                        if not non_tool:
                            is_tool_result_only = True
                    if not is_tool_result_only:
                        turn_count += 1

                # Count tool calls and failures
                if isinstance(content, list):
                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        if block.get("type") == "tool_use":
                            tool_call_count += 1
                        if block.get("type") == "tool_result":
                            if block.get("is_error"):
                                tool_failure_count += 1

                # Accumulate token usage from assistant messages
                if entry_type == "assistant":
                    usage = msg.get("usage", {})
                    if usage:
                        input_tokens += usage.get("input_tokens", 0) or 0
                        output_tokens += usage.get("output_tokens", 0) or 0
                        cache_creation_tokens += usage.get("cache_creation_input_tokens", 0) or 0
                        cache_read_tokens += usage.get("cache_read_input_tokens", 0) or 0
                        if usage.get("speed") == "fast":
                            fast_mode_turns += 1

                # Build per-turn message list for reprompt detection
                messages.append({"role": role, "content": content})

                # Build raw_content_json (stripped of thinking/tool_use/tool_result)
                stripped = _strip_content_blocks(content)
                if stripped is not None and stripped != [] and stripped != "":
                    raw_content_turns.append({"role": role, "content": stripped})

    except OSError as exc:
        raise SessionParseError(f"Cannot read session file {filepath}: {exc}") from exc

    if session_uuid is None:
        raise SessionParseError(
            f"No sessionId found in {filepath} — file may be empty or not a session log"
        )

    # If no git branch from JSONL metadata, try resolving from working directory
    if git_branch is None and working_directory:
        git_branch = _git_branch_from_dir(working_directory)

    # Calculate duration
    duration = 0.0
    if len(timestamps) >= 2:
        duration = (timestamps[-1] - timestamps[0]).total_seconds()

    # Session timestamp anchors for Phase-2 synthesis (Epic #17 Decision 1).
    # We intentionally use ``timestamps`` unsorted — they're appended in file
    # order, which is already wall-clock ascending. Even if they were not,
    # taking min/max would be equivalent. Falling back to None when the file
    # had no timestamped entries keeps the column NULL-able and matches the
    # schema default set in db.py.
    if timestamps:
        session_started_at = timestamps[0].isoformat()
        session_ended_at = timestamps[-1].isoformat()
    else:
        session_started_at = None
        session_ended_at = None

    # Detect reprompts
    reprompt_count, bail_out = detect_reprompts(
        messages, threshold=threshold
    )

    raw_content_json = json.dumps(raw_content_turns, ensure_ascii=False)

    return SessionRecord(
        session_uuid=session_uuid,
        turn_count=turn_count,
        tool_call_count=tool_call_count,
        tool_failure_count=tool_failure_count,
        reprompt_count=reprompt_count,
        bail_out=bail_out,
        session_duration_seconds=duration,
        working_directory=working_directory,
        git_branch=git_branch,
        raw_content_json=raw_content_json,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_tokens=cache_creation_tokens,
        cache_read_tokens=cache_read_tokens,
        fast_mode_turns=fast_mode_turns,
        session_started_at=session_started_at,
        session_ended_at=session_ended_at,
    )


def process_session(
    filepath: str | Path,
    db_path: Optional[str | Path] = None,
    data_dir: Optional[str | Path] = None,
    threshold: int = 3,
) -> str:
    """Parse a session file, detect reprompts, store in DB, write health.

    Returns the session_uuid.
    """
    record = parse_session(filepath, threshold=threshold)
    upsert_session(record, db_path=db_path, data_dir=data_dir)
    return record.session_uuid


def _load_messages(filepath: str | Path) -> List[Dict[str, Any]]:
    """Load user/assistant messages from a session file."""
    filepath = Path(filepath)
    messages = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue  # skip malformed lines
                if entry.get("type") not in ("user", "assistant"):
                    continue
                msg = entry.get("message", {})
                messages.append(
                    {"role": msg.get("role", entry["type"]), "content": msg.get("content", "")}
                )
    except OSError as exc:
        raise SessionParseError(f"Cannot read session file {filepath}: {exc}") from exc
    return messages


def _discover_session_files(projects_path: str | Path) -> List[Path]:
    """Find all .jsonl session files under the projects directory.

    Excludes subagent files (in subagents/ subdirectories).
    """
    projects_path = Path(projects_path)
    if not projects_path.exists():
        return []

    files = []
    for jsonl in projects_path.rglob("*.jsonl"):
        # Skip subagent sessions
        if "subagents" in jsonl.parts:
            continue
        files.append(jsonl)
    return sorted(files)


def _get_existing_uuids(db_path: Path) -> set:
    """Get set of session_uuids already in the database."""
    import sqlite3

    if not db_path.exists():
        return set()
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("SELECT session_uuid FROM sessions")
        return {row[0] for row in cursor.fetchall()}
    except sqlite3.OperationalError:
        return set()
    finally:
        conn.close()


def _extract_uuid_from_file(filepath: Path) -> Optional[str]:
    """Quick extraction of sessionId without full parse."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                entry = json.loads(line)
                if "sessionId" in entry:
                    return entry["sessionId"]
    except (json.JSONDecodeError, OSError):
        pass
    return None


def run_hook(session_file: str, config_path: Optional[str] = None) -> None:
    """Hook mode: process a single session file."""
    from am_i_shipping.config_loader import load_config

    config = load_config(config_path)
    db_path = config.data_path / "sessions.db"

    uuid = process_session(
        session_file,
        db_path=db_path,
        data_dir=config.data_path,
        threshold=config.session.reprompt_threshold,
    )
    print(uuid)


def run_batch(config_path: Optional[str] = None) -> None:
    """Batch mode: process all unprocessed sessions."""
    from am_i_shipping.config_loader import load_config

    config = load_config(config_path)
    db_path = config.data_path / "sessions.db"
    data_dir = config.data_path

    # Read limiter config
    max_files = config.session.limiter.max_files_per_run
    inter_delay = config.session.limiter.inter_file_delay_seconds

    # Ensure DB exists
    from am_i_shipping.db import init_sessions_db

    data_dir.mkdir(parents=True, exist_ok=True)
    init_sessions_db(db_path)

    # Discover files
    session_files = _discover_session_files(config.session.projects_path)
    if not session_files:
        print("No session files found", file=sys.stderr)
        from am_i_shipping.health_writer import write_health

        write_health("session_parser", 0, data_dir=data_dir)
        return

    # Get already-processed UUIDs
    existing = _get_existing_uuids(db_path)

    processed = 0
    skipped = 0
    errors = 0

    for sf in session_files:
        # Enforce per-run file cap
        if processed >= max_files:
            print(
                f"  Reached max_files_per_run={max_files}, deferring rest to next run",
                file=sys.stderr,
            )
            break

        # Quick check: extract UUID and skip if already in DB
        uuid = _extract_uuid_from_file(sf)
        if uuid and uuid in existing:
            skipped += 1
            continue

        try:
            record = parse_session(
                sf, threshold=config.session.reprompt_threshold
            )
            upsert_session(
                record,
                db_path=db_path,
                data_dir=data_dir,
                skip_init=True,
                skip_health=True,
            )
            existing.add(record.session_uuid)
            processed += 1
        except SessionParseError as exc:
            print(f"WARN: {sf}: {exc}", file=sys.stderr)
            errors += 1

        # Inter-file delay
        if inter_delay > 0:
            time.sleep(inter_delay)

    print(
        f"Batch complete: {processed} processed, {skipped} skipped, {errors} errors",
        file=sys.stderr,
    )

    from am_i_shipping.health_writer import write_health

    write_health("session_parser", processed, data_dir=data_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Claude Code session JSONL files"
    )
    parser.add_argument(
        "--mode",
        choices=["hook", "batch"],
        required=True,
        help="hook: process single file; batch: process all under projects_path",
    )
    parser.add_argument(
        "--session-file",
        default=None,
        help="Path to session JSONL file (required for hook mode)",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml",
    )
    args = parser.parse_args()

    if args.mode == "hook":
        if not args.session_file:
            parser.error("--session-file is required for hook mode")
        run_hook(args.session_file, config_path=args.config)
    elif args.mode == "batch":
        run_batch(config_path=args.config)


if __name__ == "__main__":
    main()
