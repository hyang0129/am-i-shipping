"""Idempotent database initialization for all three collector DBs.

Usage:
    python -m am_i_shipping.db [--config path/to/config.yaml]

Creates sessions.db, github.db, and appswitch.db under the configured data
directory. Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from .config_loader import Config, load_config


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------

SESSIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_uuid    TEXT PRIMARY KEY,
    turn_count      INTEGER,
    tool_call_count INTEGER,
    tool_failure_count INTEGER,
    reprompt_count  INTEGER,
    bail_out        INTEGER DEFAULT 0,
    session_duration_seconds REAL,
    working_directory TEXT,
    git_branch      TEXT,
    raw_content_json TEXT,
    input_tokens    INTEGER DEFAULT 0,
    output_tokens   INTEGER DEFAULT 0,
    cache_creation_tokens INTEGER DEFAULT 0,
    cache_read_tokens INTEGER DEFAULT 0,
    fast_mode_turns INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);
"""

# Columns added after the initial schema — migrated on init
_SESSIONS_MIGRATIONS = [
    "ALTER TABLE sessions ADD COLUMN input_tokens INTEGER DEFAULT 0",
    "ALTER TABLE sessions ADD COLUMN output_tokens INTEGER DEFAULT 0",
    "ALTER TABLE sessions ADD COLUMN cache_creation_tokens INTEGER DEFAULT 0",
    "ALTER TABLE sessions ADD COLUMN cache_read_tokens INTEGER DEFAULT 0",
    "ALTER TABLE sessions ADD COLUMN fast_mode_turns INTEGER DEFAULT 0",
]

GITHUB_ISSUES_SCHEMA = """
CREATE TABLE IF NOT EXISTS issues (
    repo            TEXT NOT NULL,
    issue_number    INTEGER NOT NULL,
    title           TEXT,
    type_label      TEXT,
    state           TEXT,
    body            TEXT,
    comments_json   TEXT,
    created_at      TEXT,
    closed_at       TEXT,
    updated_at      TEXT,
    PRIMARY KEY (repo, issue_number)
);
"""

GITHUB_PRS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pull_requests (
    repo                TEXT NOT NULL,
    pr_number           INTEGER NOT NULL,
    head_ref            TEXT,
    title               TEXT,
    body                TEXT,
    comments_json        TEXT,
    review_comments_json TEXT,
    review_comment_count INTEGER DEFAULT 0,
    push_count          INTEGER DEFAULT 0,
    created_at          TEXT,
    merged_at           TEXT,
    updated_at          TEXT,
    PRIMARY KEY (repo, pr_number)
);
"""

GITHUB_PR_ISSUES_SCHEMA = """
CREATE TABLE IF NOT EXISTS pr_issues (
    repo            TEXT NOT NULL,
    pr_number       INTEGER NOT NULL,
    issue_number    INTEGER NOT NULL,
    PRIMARY KEY (repo, pr_number, issue_number)
);
"""

GITHUB_PR_SESSIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pr_sessions (
    repo            TEXT NOT NULL,
    pr_number       INTEGER NOT NULL,
    session_uuid    TEXT NOT NULL,
    PRIMARY KEY (repo, pr_number, session_uuid)
);
"""

GITHUB_CURSOR_SCHEMA = """
CREATE TABLE IF NOT EXISTS poll_cursor (
    repo            TEXT PRIMARY KEY,
    last_polled_at  TEXT NOT NULL
);
"""

GITHUB_ISSUE_BODY_EDITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS issue_body_edits (
    repo        TEXT NOT NULL,
    issue_number INTEGER NOT NULL,
    edited_at   TEXT NOT NULL,
    diff        TEXT,
    editor      TEXT,
    PRIMARY KEY (repo, issue_number, edited_at)
);
"""

GITHUB_ISSUE_COMMENT_EDITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS issue_comment_edits (
    repo         TEXT NOT NULL,
    issue_number INTEGER NOT NULL,
    comment_id   INTEGER NOT NULL,
    edited_at    TEXT NOT NULL,
    diff         TEXT,
    editor       TEXT,
    PRIMARY KEY (repo, issue_number, comment_id, edited_at)
);
"""

GITHUB_PR_BODY_EDITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pr_body_edits (
    repo       TEXT NOT NULL,
    pr_number  INTEGER NOT NULL,
    edited_at  TEXT NOT NULL,
    diff        TEXT,
    editor      TEXT,
    PRIMARY KEY (repo, pr_number, edited_at)
);
"""

GITHUB_PR_REVIEW_COMMENT_EDITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pr_review_comment_edits (
    repo        TEXT NOT NULL,
    pr_number   INTEGER NOT NULL,
    comment_id  INTEGER NOT NULL,
    edited_at   TEXT NOT NULL,
    diff         TEXT,
    editor       TEXT,
    PRIMARY KEY (repo, pr_number, comment_id, edited_at)
);
"""

# Columns added to existing tables after initial schema — migrated on init
_GITHUB_MIGRATIONS = [
    "ALTER TABLE issues ADD COLUMN updated_at TEXT",
    "ALTER TABLE pull_requests ADD COLUMN updated_at TEXT",
    "ALTER TABLE pull_requests ADD COLUMN comments_json TEXT",
]

APPSWITCH_SCHEMA = """
CREATE TABLE IF NOT EXISTS app_events (
    timestamp_bucket INTEGER NOT NULL,
    window_hash     TEXT NOT NULL,
    app_name        TEXT,
    window_title    TEXT,
    duration_seconds REAL,
    PRIMARY KEY (timestamp_bucket, window_hash)
);
"""


def init_sessions_db(db_path: Path) -> None:
    """Create sessions.db with the sessions table, running any pending migrations."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(SESSIONS_SCHEMA)
        for migration in _SESSIONS_MIGRATIONS:
            try:
                conn.execute(migration)
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.commit()
    finally:
        conn.close()


def init_github_db(db_path: Path) -> None:
    """Create github.db with issues, PRs, linkage, cursor, and edit history tables."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(GITHUB_ISSUES_SCHEMA)
        conn.execute(GITHUB_PRS_SCHEMA)
        conn.execute(GITHUB_PR_ISSUES_SCHEMA)
        conn.execute(GITHUB_PR_SESSIONS_SCHEMA)
        conn.execute(GITHUB_CURSOR_SCHEMA)
        conn.execute(GITHUB_ISSUE_BODY_EDITS_SCHEMA)
        conn.execute(GITHUB_ISSUE_COMMENT_EDITS_SCHEMA)
        conn.execute(GITHUB_PR_BODY_EDITS_SCHEMA)
        conn.execute(GITHUB_PR_REVIEW_COMMENT_EDITS_SCHEMA)
        for migration in _GITHUB_MIGRATIONS:
            try:
                conn.execute(migration)
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.commit()
    finally:
        conn.close()


def init_appswitch_db(db_path: Path) -> None:
    """Create appswitch.db with the app_events table."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(APPSWITCH_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def init_all(config: Config) -> None:
    """Initialize all three databases under the configured data directory."""
    data_dir = config.data_path
    data_dir.mkdir(parents=True, exist_ok=True)

    init_sessions_db(data_dir / "sessions.db")
    init_github_db(data_dir / "github.db")
    init_appswitch_db(data_dir / "appswitch.db")

    print(f"Databases initialized in {data_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize collector databases")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: config.yaml in repo root)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    init_all(config)


if __name__ == "__main__":
    main()
