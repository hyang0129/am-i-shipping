"""SQLite storage for parsed session records.

Provides idempotent upsert of SessionRecord into sessions.db and
writes health status on success.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

from am_i_shipping.health_writer import write_health

if TYPE_CHECKING:
    from .session_parser import SessionRecord


def upsert_session(
    record: "SessionRecord",
    db_path: Optional[Union[str, Path]] = None,
    data_dir: Optional[Union[str, Path]] = None,
    skip_init: bool = False,
    skip_health: bool = False,
) -> None:
    """Insert or update a session record in sessions.db.

    Idempotent: if a record with the same session_uuid already exists,
    it is updated with the new values.

    Parameters
    ----------
    record:
        Parsed session data.
    db_path:
        Path to sessions.db. If None, uses data_dir / "sessions.db".
    data_dir:
        Directory for health.json. Defaults to repo-root/data/.
    skip_init:
        If True, skip ``init_sessions_db()`` call. Use when the caller
        has already initialized the schema (e.g. batch mode).
    skip_health:
        If True, skip ``write_health()`` call. Use when the caller
        writes health once at the end of a batch.
    """
    if data_dir is None:
        data_dir = Path(__file__).resolve().parent.parent / "data"
    else:
        data_dir = Path(data_dir)

    if db_path is None:
        db_path = data_dir / "sessions.db"
    else:
        db_path = Path(db_path)

    # Ensure the DB directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Ensure table exists (unless caller already did this)
    if not skip_init:
        from am_i_shipping.db import init_sessions_db

        init_sessions_db(db_path)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO sessions (
                session_uuid, turn_count, tool_call_count,
                tool_failure_count, reprompt_count, bail_out,
                session_duration_seconds, working_directory,
                git_branch, raw_content_json,
                input_tokens, output_tokens, cache_creation_tokens,
                cache_read_tokens, fast_mode_turns,
                session_started_at, session_ended_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_uuid) DO UPDATE SET
                turn_count = excluded.turn_count,
                tool_call_count = excluded.tool_call_count,
                tool_failure_count = excluded.tool_failure_count,
                reprompt_count = excluded.reprompt_count,
                bail_out = excluded.bail_out,
                session_duration_seconds = excluded.session_duration_seconds,
                working_directory = excluded.working_directory,
                git_branch = excluded.git_branch,
                raw_content_json = excluded.raw_content_json,
                input_tokens = excluded.input_tokens,
                output_tokens = excluded.output_tokens,
                cache_creation_tokens = excluded.cache_creation_tokens,
                cache_read_tokens = excluded.cache_read_tokens,
                fast_mode_turns = excluded.fast_mode_turns,
                session_started_at = excluded.session_started_at,
                session_ended_at = excluded.session_ended_at
            """,
            (
                record.session_uuid,
                record.turn_count,
                record.tool_call_count,
                record.tool_failure_count,
                record.reprompt_count,
                int(record.bail_out),
                record.session_duration_seconds,
                record.working_directory,
                record.git_branch,
                record.raw_content_json,
                record.input_tokens,
                record.output_tokens,
                record.cache_creation_tokens,
                record.cache_read_tokens,
                record.fast_mode_turns,
                record.session_started_at,
                record.session_ended_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    # Persist gh_events to github.db if present
    gh_events = getattr(record, "gh_events", None) or []
    if gh_events:
        github_db_path = db_path.parent / "github.db"
        if github_db_path.exists():
            if not skip_init:
                from am_i_shipping.db import init_github_db

                init_github_db(github_db_path)
            gh_conn = sqlite3.connect(str(github_db_path))
            try:
                for ev in gh_events:
                    gh_conn.execute(
                        "INSERT OR IGNORE INTO session_gh_events "
                        "(session_uuid, event_type, repo, ref, url, confidence, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            record.session_uuid,
                            ev["event_type"],
                            ev["repo"],
                            ev["ref"],
                            ev.get("url"),
                            ev.get("confidence"),
                            ev.get("created_at"),
                        ),
                    )
                gh_conn.commit()
            finally:
                gh_conn.close()

    if not skip_health:
        write_health("session_parser", 1, data_dir=data_dir)
