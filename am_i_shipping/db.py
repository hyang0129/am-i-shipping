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
    # Epic #17 — Sub-Issue 1 (Decision 1): session timestamp columns for synthesis.
    # Populated by session_parser on new inserts; historical rows backfilled via
    # am_i_shipping/scripts/backfill_session_timestamps.py (Sub-Issue 2).
    "ALTER TABLE sessions ADD COLUMN session_started_at TEXT",
    "ALTER TABLE sessions ADD COLUMN session_ended_at TEXT",
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

# ---------------------------------------------------------------------------
# Synthesis tables (Epic #17 — Sub-Issue 1)
# Stored in github.db alongside the existing GitHub tables per the epic ADR.
# All additive — no Phase 1 table is rewritten. Populated by later sub-issues:
#   commits / timeline_events  -> Sub-Issue 3 (new fetchers)
#   graph_nodes / graph_edges  -> Sub-Issue 4 (graph builder)
#   units                      -> Sub-Issue 5 (unit identifier; persistent,
#                                 append-only, keyed on (week_start, unit_id))
# ---------------------------------------------------------------------------

SYNTHESIS_COMMITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS commits (
    repo        TEXT NOT NULL,
    sha         TEXT NOT NULL,
    author      TEXT,
    authored_at TEXT,
    message     TEXT,
    pr_number   INTEGER,
    pushed_at   TEXT,
    PRIMARY KEY (repo, sha)
);
"""

SYNTHESIS_TIMELINE_EVENTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS timeline_events (
    repo         TEXT NOT NULL,
    issue_number INTEGER NOT NULL,
    event_id     INTEGER NOT NULL,
    event_type   TEXT,
    actor        TEXT,
    created_at   TEXT,
    payload_json TEXT,
    PRIMARY KEY (repo, issue_number, event_id)
);
"""

SYNTHESIS_GRAPH_NODES_SCHEMA = """
CREATE TABLE IF NOT EXISTS graph_nodes (
    week_start  TEXT NOT NULL,
    node_id     TEXT NOT NULL,
    node_type   TEXT,
    node_ref    TEXT,
    created_at  TEXT,
    PRIMARY KEY (week_start, node_id)
);
"""

SYNTHESIS_GRAPH_EDGES_SCHEMA = """
CREATE TABLE IF NOT EXISTS graph_edges (
    week_start   TEXT NOT NULL,
    src_node_id  TEXT NOT NULL,
    dst_node_id  TEXT NOT NULL,
    edge_type    TEXT NOT NULL,
    PRIMARY KEY (week_start, src_node_id, dst_node_id, edge_type)
);
"""

SYNTHESIS_UNITS_SCHEMA = """
CREATE TABLE IF NOT EXISTS units (
    week_start      TEXT NOT NULL,
    unit_id         TEXT NOT NULL,
    root_node_type  TEXT,
    root_node_id    TEXT,
    elapsed_days    REAL,
    dark_time_pct   REAL,
    total_reprompts INTEGER,
    review_cycles   INTEGER,
    status          TEXT,
    PRIMARY KEY (week_start, unit_id)
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


# ---------------------------------------------------------------------------
# Expected-column registry — drives _assert_schema()
# ---------------------------------------------------------------------------
#
# These constants describe the columns that MUST exist after init_*_db() has
# run (both fresh creation and migration replay). They exist so silent
# OperationalError swallowing in the migration loops cannot hide a missed
# ALTER TABLE. Tests import these constants directly to keep the source of
# truth in one place.

_EXPECTED_SESSIONS_COLUMNS: set[str] = {
    "session_uuid",
    "turn_count",
    "tool_call_count",
    "tool_failure_count",
    "reprompt_count",
    "bail_out",
    "session_duration_seconds",
    "working_directory",
    "git_branch",
    "raw_content_json",
    "input_tokens",
    "output_tokens",
    "cache_creation_tokens",
    "cache_read_tokens",
    "fast_mode_turns",
    "created_at",
    # Epic #17 — Sub-Issue 1 additions
    "session_started_at",
    "session_ended_at",
}

_EXPECTED_GITHUB_TABLES: dict[str, set[str]] = {
    "issues": {
        "repo",
        "issue_number",
        "title",
        "type_label",
        "state",
        "body",
        "comments_json",
        "created_at",
        "closed_at",
        "updated_at",
    },
    "pull_requests": {
        "repo",
        "pr_number",
        "head_ref",
        "title",
        "body",
        "comments_json",
        "review_comments_json",
        "review_comment_count",
        "push_count",
        "created_at",
        "merged_at",
        "updated_at",
    },
    "pr_issues": {"repo", "pr_number", "issue_number"},
    "pr_sessions": {"repo", "pr_number", "session_uuid"},
    "poll_cursor": {"repo", "last_polled_at"},
    "issue_body_edits": {
        "repo",
        "issue_number",
        "edited_at",
        "diff",
        "editor",
    },
    "issue_comment_edits": {
        "repo",
        "issue_number",
        "comment_id",
        "edited_at",
        "diff",
        "editor",
    },
    "pr_body_edits": {"repo", "pr_number", "edited_at", "diff", "editor"},
    "pr_review_comment_edits": {
        "repo",
        "pr_number",
        "comment_id",
        "edited_at",
        "diff",
        "editor",
    },
    # Epic #17 — Sub-Issue 1 additions
    "commits": {
        "repo",
        "sha",
        "author",
        "authored_at",
        "message",
        "pr_number",
        "pushed_at",
    },
    "timeline_events": {
        "repo",
        "issue_number",
        "event_id",
        "event_type",
        "actor",
        "created_at",
        "payload_json",
    },
    "graph_nodes": {
        "week_start",
        "node_id",
        "node_type",
        "node_ref",
        "created_at",
    },
    "graph_edges": {
        "week_start",
        "src_node_id",
        "dst_node_id",
        "edge_type",
    },
    "units": {
        "week_start",
        "unit_id",
        "root_node_type",
        "root_node_id",
        "elapsed_days",
        "dark_time_pct",
        "total_reprompts",
        "review_cycles",
        "status",
    },
}

_EXPECTED_APPSWITCH_TABLES: dict[str, set[str]] = {
    "app_events": {
        "timestamp_bucket",
        "window_hash",
        "app_name",
        "window_title",
        "duration_seconds",
    },
}


def _assert_schema(
    db_path: Path, expected: dict[str, set[str]]
) -> None:
    """Fail loud if any expected table/column is missing from *db_path*.

    The migration helpers above intentionally swallow ``sqlite3.OperationalError``
    so that re-running ``ALTER TABLE ADD COLUMN`` against an already-migrated DB
    is a no-op. That convenience also hides the case where the migration text
    itself was malformed — silent failure would let a collector write against a
    schema that quietly lacks the column. ``_assert_schema`` closes that loop by
    reading ``PRAGMA table_info`` for each expected table and raising a
    ``RuntimeError`` that names the specific missing column.

    Parameters
    ----------
    db_path:
        Path to the SQLite database to verify. Must already exist.
    expected:
        Mapping of ``table_name -> set_of_expected_column_names``. A table is
        considered present iff it appears in ``sqlite_master``; a column is
        considered present iff it appears in ``PRAGMA table_info(<table>)``.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        existing_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        for table, expected_columns in expected.items():
            if table not in existing_tables:
                raise RuntimeError(
                    f"Schema assertion failed for {db_path}: "
                    f"missing table {table!r}"
                )
            actual_columns = {
                row[1]
                for row in conn.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            missing = expected_columns - actual_columns
            if missing:
                raise RuntimeError(
                    f"Schema assertion failed for {db_path}: "
                    f"table {table!r} missing columns "
                    f"{sorted(missing)!r}"
                )
    finally:
        conn.close()


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
    _assert_schema(db_path, {"sessions": _EXPECTED_SESSIONS_COLUMNS})


def init_github_db(db_path: Path) -> None:
    """Create github.db with issues, PRs, linkage, cursor, edit history, and synthesis tables."""
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
        # Synthesis tables (Epic #17 — Sub-Issue 1)
        conn.execute(SYNTHESIS_COMMITS_SCHEMA)
        conn.execute(SYNTHESIS_TIMELINE_EVENTS_SCHEMA)
        conn.execute(SYNTHESIS_GRAPH_NODES_SCHEMA)
        conn.execute(SYNTHESIS_GRAPH_EDGES_SCHEMA)
        conn.execute(SYNTHESIS_UNITS_SCHEMA)
        for migration in _GITHUB_MIGRATIONS:
            try:
                conn.execute(migration)
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.commit()
    finally:
        conn.close()
    _assert_schema(db_path, _EXPECTED_GITHUB_TABLES)


def init_appswitch_db(db_path: Path) -> None:
    """Create appswitch.db with the app_events table."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(APPSWITCH_SCHEMA)
        conn.commit()
    finally:
        conn.close()
    _assert_schema(db_path, _EXPECTED_APPSWITCH_TABLES)


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
