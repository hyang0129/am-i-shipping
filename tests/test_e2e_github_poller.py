"""E2E tests for the GitHub poller against a real GitHub repo.

Guards
------
Requires ``AM_I_SHIPPING_E2E=1`` in the environment.  The ``gh`` CLI must be
authenticated.  A temporary issue is created on the target repo for each test
class run and closed in teardown.

What is tested
--------------
1. A new issue appears in the DB after the first poll.
2. Re-polling produces no duplicate rows (upsert is idempotent).
3. Editing the issue body is reflected on the next poll; old text is not
   preserved (known gap — no history table).
4. A new comment appears in ``comments_json`` after a re-poll.
5. Editing a comment is reflected; no duplicate comment entry is created;
   original text is not preserved; ``updated_at`` is absent from the stored
   comment (known gap — fetch_issues does not capture it).

Cursor note
-----------
``advance_cursor`` sets ``last_polled_at`` to today.  GitHub's
``updated:>DATE`` filter is strictly greater than, so same-day edits would
be missed on a real incremental poll.  Tests call ``_reset_cursor`` (rewinds
to yesterday) before each re-poll so edits made in the test run are caught.
This is intentional: the cursor-skipping behaviour is a separate known issue
that should be addressed in the cursor design, not papered over here.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

# ---------------------------------------------------------------------------
# Constants / environment
# ---------------------------------------------------------------------------

REPO = os.environ.get("GITHUB_E2E_REPO", "hyang0129/am-i-shipping")
PROJECTS_PATH = Path(
    os.environ.get(
        "AM_I_SHIPPING_PROJECTS_PATH",
        str(Path.home() / ".claude" / "projects"),
    )
)

pytestmark = pytest.mark.skipif(
    os.environ.get("AM_I_SHIPPING_E2E", "0") != "1",
    reason="E2E GitHub poller tests require AM_I_SHIPPING_E2E=1 and gh CLI auth",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _gh(*args: str) -> str:
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def _gh_json(*args: str) -> Any:
    return json.loads(_gh(*args))


def _write_config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
session:
  projects_path: "{PROJECTS_PATH}"
github:
  repos:
    - {REPO}
  backfill_days: 7
data:
  data_dir: "{tmp_path / 'data'}"
""".lstrip()
    )
    return config


def _poll(config_path: Path) -> tuple[int, bool]:
    from collector.github_poller.run import run

    return run(config_path=str(config_path))


def _reset_cursor(db_path: Path, repo: str) -> None:
    """Rewind the poll cursor to yesterday.

    GitHub's ``updated:>DATE`` filter is strictly greater-than, so issues
    updated *today* would be missed if the cursor is set to today.  Rewinding
    to yesterday ensures same-day edits are picked up in tests.
    """
    yesterday = (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        INSERT INTO poll_cursor (repo, last_polled_at) VALUES (?, ?)
        ON CONFLICT(repo) DO UPDATE SET last_polled_at = excluded.last_polled_at
        """,
        (repo, yesterday),
    )
    conn.commit()
    conn.close()


def _get_issue(
    db_path: Path, repo: str, issue_number: int
) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM issues WHERE repo = ? AND issue_number = ?",
        (repo, issue_number),
    ).fetchone()
    conn.close()
    if row is None:
        return None
    d = dict(row)
    d["comments"] = json.loads(d.get("comments_json") or "[]")
    return d


def _issue_row_count(db_path: Path, repo: str, issue_number: int) -> int:
    conn = sqlite3.connect(str(db_path))
    n = conn.execute(
        "SELECT COUNT(*) FROM issues WHERE repo = ? AND issue_number = ?",
        (repo, issue_number),
    ).fetchone()[0]
    conn.close()
    return n


# ---------------------------------------------------------------------------
# Fixture: real GitHub issue, closed on teardown
# ---------------------------------------------------------------------------


@pytest.fixture(scope="class")
def live_issue(tmp_path_factory) -> int:
    """Create a real GitHub issue; yield its number; close it on teardown."""
    issue_url = _gh(
        "issue", "create",
        "--repo", REPO,
        "--title", "[e2e-test] GitHub poller lifecycle test",
        "--body", "Initial body text. Created by e2e test.",
    )
    issue_number = int(issue_url.rstrip("/").split("/")[-1])
    yield issue_number
    # Cleanup: close the issue (delete requires admin; close is sufficient)
    try:
        _gh(
            "issue", "close", str(issue_number),
            "--repo", REPO,
            "--comment", "Closed by e2e test cleanup.",
        )
    except subprocess.CalledProcessError:
        pass  # best-effort; don't fail the test suite on cleanup error


@pytest.fixture(scope="class")
def class_tmp_path(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("github_poller_e2e")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGitHubPollerE2E:
    """End-to-end lifecycle tests for the GitHub poller."""

    def test_new_issue_appears_after_poll(self, class_tmp_path, live_issue):
        """A newly created issue is persisted after the first poll."""
        config = _write_config(class_tmp_path)
        db_path = class_tmp_path / "data" / "github.db"

        total, ok = _poll(config)

        assert ok, "Poller reported failure"
        assert total > 0, "Poller fetched zero records"

        row = _get_issue(db_path, REPO, live_issue)
        assert row is not None, f"Issue #{live_issue} not found in DB after first poll"
        assert row["title"] == "[e2e-test] GitHub poller lifecycle test"
        assert "Initial body text" in row["body"]
        assert row["state"] == "OPEN"

    def test_no_duplicate_rows_on_repoll(self, class_tmp_path, live_issue):
        """Re-polling the same data produces exactly one row for the test issue.

        Note: total row count may legitimately increase if other issues were
        updated on GitHub between runs; we only assert on the test issue itself.
        """
        config = _write_config(class_tmp_path)
        db_path = class_tmp_path / "data" / "github.db"

        # Ensure DB is populated from a previous test or seed here
        _poll(config)

        # Rewind cursor and re-poll
        _reset_cursor(db_path, REPO)
        _poll(config)

        count = _issue_row_count(db_path, REPO, live_issue)
        assert count == 1, (
            f"Expected exactly 1 row for issue #{live_issue}, found {count}"
        )

    def test_issue_body_edit_is_reflected(self, class_tmp_path, live_issue):
        """Editing the issue body updates the stored row on the next poll.

        Known gap: old body text is not preserved — the upsert overwrites it
        with no history table.  This test documents that gap explicitly.
        """
        config = _write_config(class_tmp_path)
        db_path = class_tmp_path / "data" / "github.db"

        # Seed poll
        _poll(config)
        before = _get_issue(db_path, REPO, live_issue)
        assert before is not None
        assert "Initial body text" in before["body"]

        # Edit body via gh CLI
        _gh(
            "issue", "edit", str(live_issue),
            "--repo", REPO,
            "--body", "EDITED body text. Updated by e2e test.",
        )

        _reset_cursor(db_path, REPO)
        _poll(config)

        after = _get_issue(db_path, REPO, live_issue)
        assert after is not None

        # Updated text is present
        assert "EDITED body text" in after["body"], (
            "Updated body was not reflected in DB after re-poll"
        )

        # Known gap: original text is gone — no history preserved
        assert "Initial body text" not in after["body"], (
            "Old body text unexpectedly still present — "
            "upsert should have overwritten it"
        )

    def test_new_comment_appears_after_poll(self, class_tmp_path, live_issue):
        """A comment added to an issue appears in ``comments_json`` after re-poll."""
        config = _write_config(class_tmp_path)
        db_path = class_tmp_path / "data" / "github.db"

        _poll(config)
        before = _get_issue(db_path, REPO, live_issue)
        assert before is not None
        initial_count = len(before["comments"])

        # Add a comment
        _gh(
            "issue", "comment", str(live_issue),
            "--repo", REPO,
            "--body", "First comment. Original text before any edit.",
        )

        _reset_cursor(db_path, REPO)
        _poll(config)

        after = _get_issue(db_path, REPO, live_issue)
        assert after is not None

        assert len(after["comments"]) == initial_count + 1, (
            "New comment did not appear in comments_json after re-poll"
        )
        bodies = [c["body"] for c in after["comments"]]
        assert any("First comment" in b for b in bodies), (
            "Comment body not found in stored comments"
        )

    def test_comment_edit_reflected_no_duplicate_no_history(
        self, class_tmp_path, live_issue
    ):
        """Editing a comment updates the stored text on re-poll.

        Asserts:
        - Edited text appears in ``comments_json``
        - Comment count does not increase (no duplication)

        Known gaps documented:
        - Original comment text is not preserved (overwritten by upsert)
        - ``updated_at`` is absent from stored comment dicts (not fetched)
          — if this assertion fails, the gap has been fixed; remove it
        """
        config = _write_config(class_tmp_path)
        db_path = class_tmp_path / "data" / "github.db"

        # Seed: add a comment and poll to capture it
        _gh(
            "issue", "comment", str(live_issue),
            "--repo", REPO,
            "--body", "Comment before edit. ORIGINAL TEXT.",
        )
        _reset_cursor(db_path, REPO)
        _poll(config)

        before = _get_issue(db_path, REPO, live_issue)
        assert before is not None
        count_before = len(before["comments"])
        assert any("ORIGINAL TEXT" in c["body"] for c in before["comments"]), (
            "Seeded comment not found in DB before edit"
        )

        # Fetch the GitHub comment ID so we can PATCH it
        owner, name = REPO.split("/", 1)
        raw_comments: List[Dict] = _gh_json(
            "api", f"/repos/{owner}/{name}/issues/{live_issue}/comments"
        )
        gh_comment = next(
            c for c in raw_comments if "ORIGINAL TEXT" in c["body"]
        )
        comment_id = gh_comment["id"]

        # Edit the comment via gh api PATCH
        _gh(
            "api", "--method", "PATCH",
            f"/repos/{owner}/{name}/issues/comments/{comment_id}",
            "--field", "body=Comment after edit. CHANGED TEXT.",
        )

        _reset_cursor(db_path, REPO)
        _poll(config)

        after = _get_issue(db_path, REPO, live_issue)
        assert after is not None
        bodies = [c["body"] for c in after["comments"]]

        # Edited text appears
        assert any("CHANGED TEXT" in b for b in bodies), (
            "Edited comment text not reflected in comments_json after re-poll"
        )

        # No duplicate entries
        assert len(after["comments"]) == count_before, (
            f"Comment count changed from {count_before} to {len(after['comments'])} "
            "after comment edit — possible duplication in comments_json"
        )

        # Known gap: original text is gone
        assert not any("ORIGINAL TEXT" in b for b in bodies), (
            "Original comment text still present — expected upsert to overwrite it"
        )

        # Known gap: no updated_at on stored comments
        for c in after["comments"]:
            assert "updated_at" not in c, (
                "updated_at present on stored comment — "
                "remove this assertion if the gap is intentionally fixed"
            )
