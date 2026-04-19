"""Build tests/fixtures/synthesis/golden.sqlite — the deterministic fixture
for Epic #17 (Weekly Synthesis Engine).

The fixture encodes a three-unit topology so synthesis-pipeline tests
(Sub-Issues 2–7) can load a small SQLite file instead of stubbing collector
readers.  The three units are:

* Unit 1 — *multi-session / multi-PR*.  Two sessions, two merged PRs, one
  issue, `pr_sessions` links.  Sessions span a 3-day window with populated
  ``session_started_at`` / ``session_ended_at``.  Week-over-week diffing
  and outlier detection exercise this unit.
* Unit 2 — *abandoned*.  One open issue, one closed-unmerged PR, no
  sessions within 14+ days.  Abandonment flag tests exercise this unit.
* Unit 3 — *unattributed session*.  One session not linked to any PR or issue.
  Per #66, session-only components are NOT written to ``units``.  The session
  row and graph node still exist for audit purposes.

Determinism
-----------
Running this script twice produces a byte-identical ``golden.sqlite``.
This is enforced by:

* Fixed timestamps, fixed UUIDs, fixed shas.
* Deleting the existing fixture before rebuild.
* Calling ``PRAGMA journal_mode=DELETE`` before writing — prevents a
  ``-wal`` sidecar file from landing next to the committed ``.sqlite``.
* Inserting rows in a fixed, deterministic order.

Regenerate
----------
From the repo root::

    python tests/fixtures/synthesis/build_golden.py

Then ``git add tests/fixtures/synthesis/golden.sqlite`` and commit.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from am_i_shipping.db import init_github_db

FIXTURE_DIR = Path(__file__).resolve().parent
FIXTURE_PATH = FIXTURE_DIR / "golden.sqlite"

# All synthesis rows are keyed on a single week so diffing tests can anchor
# assertions.  2025-01-06 is a Monday, matching SynthesisConfig default
# week_start="monday".
WEEK_START = "2025-01-06"
REPO = "example/repo"

# ---------------------------------------------------------------------------
# Unit 1 — multi-session / multi-PR
# ---------------------------------------------------------------------------

UNIT1_SESSIONS = [
    (
        "00000000-0000-0000-0000-000000000101",  # session_uuid
        42,  # turn_count
        12,  # tool_call_count
        0,   # tool_failure_count
        1,   # reprompt_count
        0,   # bail_out
        7200.0,  # session_duration_seconds
        "/repos/example",  # working_directory
        "main",  # git_branch
        '{"turns": []}',  # raw_content_json
        1500, 2200, 800, 1800, 3,  # token counters + fast_mode_turns
        "2025-01-06T09:00:00Z",
        "2025-01-06T11:00:00Z",
    ),
    (
        "00000000-0000-0000-0000-000000000102",
        15, 5, 0, 0, 0, 1800.0, "/repos/example", "main",
        '{"turns": []}',
        600, 900, 300, 700, 1,
        "2025-01-08T14:00:00Z",
        "2025-01-08T14:30:00Z",
    ),
]

UNIT1_ISSUE = (
    REPO, 201, "Unit 1 multi-session issue", "feature", "closed",
    "Body for unit 1", "[]",
    "2025-01-06T08:55:00Z", "2025-01-08T15:00:00Z", "2025-01-08T15:00:00Z",
)

UNIT1_PRS = [
    (
        REPO, 301, "fix/u1-a", "Unit 1 PR A", "Body A",
        "[]", "[]", 0, 1,
        "2025-01-06T10:00:00Z", "2025-01-06T12:00:00Z",
        "2025-01-06T12:00:00Z",
    ),
    (
        REPO, 302, "fix/u1-b", "Unit 1 PR B", "Body B",
        "[]", "[]", 0, 1,
        "2025-01-08T14:10:00Z", "2025-01-08T14:45:00Z",
        "2025-01-08T14:45:00Z",
    ),
]

# ---------------------------------------------------------------------------
# Unit 2 — abandoned (issue open, PR closed unmerged, no recent sessions)
# ---------------------------------------------------------------------------

UNIT2_ISSUE = (
    REPO, 202, "Unit 2 abandoned issue", "bug", "open",
    "Body for unit 2", "[]",
    "2024-12-10T10:00:00Z", None, "2024-12-11T10:00:00Z",
)

UNIT2_PR = (
    REPO, 303, "fix/u2", "Unit 2 stale PR", "Body",
    "[]", "[]", 0, 0,
    "2024-12-11T09:00:00Z", None, "2024-12-12T09:00:00Z",
)

# ---------------------------------------------------------------------------
# Unit 3 — singleton session
# ---------------------------------------------------------------------------

UNIT3_SESSION = (
    "00000000-0000-0000-0000-000000000303",
    8, 2, 0, 0, 0, 1200.0, "/repos/example", "main",
    '{"turns": []}',
    300, 400, 150, 350, 0,
    "2025-01-07T16:00:00Z",
    "2025-01-07T16:20:00Z",
)

# ---------------------------------------------------------------------------
# Commits + timeline events — minimum rows so JOINs in downstream tests have
# something to land against.
# ---------------------------------------------------------------------------

COMMITS = [
    (REPO, "a" * 40, "alice", "2025-01-06T10:05:00Z", "Unit 1 PR A commit", 301, "2025-01-06T10:06:00Z"),
    (REPO, "b" * 40, "alice", "2025-01-08T14:15:00Z", "Unit 1 PR B commit", 302, "2025-01-08T14:16:00Z"),
    (REPO, "c" * 40, "bob",   "2024-12-11T09:05:00Z", "Unit 2 stale commit", 303, "2024-12-11T09:06:00Z"),
]

TIMELINE_EVENTS = [
    (REPO, 201, 9001, "labeled",     "alice", "2025-01-06T09:00:00Z", "{}"),
    (REPO, 201, 9002, "closed",      "alice", "2025-01-08T15:00:00Z", "{}"),
    (REPO, 202, 9003, "labeled",     "bob",   "2024-12-10T10:01:00Z", "{}"),
]

# ---------------------------------------------------------------------------
# Graph nodes / edges / units — one row set per unit
# ---------------------------------------------------------------------------

GRAPH_NODES = [
    # Unit 1
    (WEEK_START, "n-u1-issue",  "issue",   f"{REPO}#201", "2025-01-06T08:55:00Z"),
    (WEEK_START, "n-u1-pr-a",   "pr",      f"{REPO}#301", "2025-01-06T10:00:00Z"),
    (WEEK_START, "n-u1-pr-b",   "pr",      f"{REPO}#302", "2025-01-08T14:10:00Z"),
    (WEEK_START, "n-u1-sess-a", "session", UNIT1_SESSIONS[0][0], "2025-01-06T09:00:00Z"),
    (WEEK_START, "n-u1-sess-b", "session", UNIT1_SESSIONS[1][0], "2025-01-08T14:00:00Z"),
    # Unit 2
    (WEEK_START, "n-u2-issue",  "issue",   f"{REPO}#202", "2024-12-10T10:00:00Z"),
    (WEEK_START, "n-u2-pr",     "pr",      f"{REPO}#303", "2024-12-11T09:00:00Z"),
    # Unit 3
    (WEEK_START, "n-u3-sess",   "session", UNIT3_SESSION[0], "2025-01-07T16:00:00Z"),
]

GRAPH_EDGES = [
    # Unit 1
    (WEEK_START, "n-u1-issue",  "n-u1-pr-a",   "closes"),
    (WEEK_START, "n-u1-issue",  "n-u1-pr-b",   "closes"),
    (WEEK_START, "n-u1-pr-a",   "n-u1-sess-a", "produced_by"),
    (WEEK_START, "n-u1-pr-b",   "n-u1-sess-b", "produced_by"),
    # Unit 2
    (WEEK_START, "n-u2-issue",  "n-u2-pr",     "closes"),
]

UNITS = [
    (WEEK_START, "unit-0001-multi",    "issue",   "n-u1-issue", 2.1, 0.89,  1, 0, "completed", None, 0),
    (WEEK_START, "unit-0002-abandoned","issue",   "n-u2-issue", 27.0, 1.0,  0, 0, "abandoned", None, 0),
    # Unit 3 (session-only) is intentionally absent: #66 requires at least one
    # issue or PR node in a component for it to become a unit.
]

UNIT_SUMMARIES = [
    (WEEK_START, "unit-0001-multi",    "Unit 1 multi-session summary: two sessions, two merged PRs, one issue resolved.", "claude-haiku-4-5", 80),
    (WEEK_START, "unit-0002-abandoned","Unit 2 abandoned summary: issue open, PR closed unmerged, no recent sessions.", "claude-haiku-4-5", 76),
]

PR_SESSIONS = [
    (REPO, 301, UNIT1_SESSIONS[0][0]),
    (REPO, 302, UNIT1_SESSIONS[1][0]),
]

PR_ISSUES = [
    (REPO, 301, 201),
    (REPO, 302, 201),
    (REPO, 303, 202),
]


def _insert_sessions(conn: sqlite3.Connection) -> None:
    """Populate sessions.  Fixture needs the schema defined in SESSIONS_SCHEMA,
    but the three sessions live in the fixture's github.db-shaped file for
    convenience (graph rows join across both worlds; the fixture is a
    single-file stand-in for the live pair).
    """
    conn.execute(
        """
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
            created_at      TEXT,
            session_started_at TEXT,
            session_ended_at   TEXT
        )
        """
    )
    rows = UNIT1_SESSIONS + [UNIT3_SESSION]
    # Source tuples are 17 elements, laid out as:
    #   cols 0..14 — primary fields + token counters + fast_mode_turns
    #   col 15    — session_started_at
    #   col 16    — session_ended_at
    # The destination sessions table has 18 columns; the extra column
    # (``created_at``) is derived at INSERT time by duplicating
    # ``session_started_at`` so the fixture stays deterministic (no
    # datetime('now') defaults creeping in). Final INSERT column order:
    #   cols 0..14 — primary fields + token counters + fast_mode_turns
    #   col 15    — created_at           (= session_started_at)
    #   col 16    — session_started_at
    #   col 17    — session_ended_at
    placeholders = ", ".join("?" * 18)
    for row in rows:
        session_started_at = row[15]
        session_ended_at = row[16]
        conn.execute(
            f"INSERT INTO sessions VALUES ({placeholders})",
            (*row[:15], session_started_at, session_started_at, session_ended_at),
        )


def build(path: Path = FIXTURE_PATH) -> None:
    """Build (or rebuild) the golden fixture at *path*.

    Determinism guarantees: identical output bytes across invocations on the
    same commit (schema stable → INSERT order stable → row bytes stable).
    """
    # Clean slate.  Also remove any WAL/shm sidecars if the previous build
    # left them on disk.
    for suffix in ("", "-wal", "-shm", "-journal"):
        sibling = path.with_name(path.name + suffix)
        if sibling.exists():
            sibling.unlink()

    # Initialise schema against the live db module so the fixture always
    # matches whatever db.py ships.  init_github_db also runs assert_schema.
    init_github_db(path)

    conn = sqlite3.connect(str(path))
    try:
        # Pin to rollback-journal mode so no -wal sidecar is produced.
        conn.execute("PRAGMA journal_mode=DELETE")

        _insert_sessions(conn)

        conn.executemany(
            "INSERT INTO issues VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [UNIT1_ISSUE, UNIT2_ISSUE],
        )
        conn.executemany(
            "INSERT INTO pull_requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            UNIT1_PRS + [UNIT2_PR],
        )
        conn.executemany(
            "INSERT INTO pr_issues VALUES (?, ?, ?)",
            PR_ISSUES,
        )
        conn.executemany(
            "INSERT INTO pr_sessions VALUES (?, ?, ?)",
            PR_SESSIONS,
        )
        conn.executemany(
            "INSERT INTO commits VALUES (?, ?, ?, ?, ?, ?, ?)",
            COMMITS,
        )
        conn.executemany(
            "INSERT INTO timeline_events VALUES (?, ?, ?, ?, ?, ?, ?)",
            TIMELINE_EVENTS,
        )
        conn.executemany(
            "INSERT INTO graph_nodes VALUES (?, ?, ?, ?, ?)",
            GRAPH_NODES,
        )
        conn.executemany(
            "INSERT INTO graph_edges VALUES (?, ?, ?, ?)",
            GRAPH_EDGES,
        )
        conn.executemany(
            "INSERT INTO units VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            UNITS,
        )
        conn.executemany(
            "INSERT INTO unit_summaries (week_start, unit_id, summary_text, model, input_bytes) VALUES (?, ?, ?, ?, ?)",
            UNIT_SUMMARIES,
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    build()
    print(f"Wrote {FIXTURE_PATH}")
