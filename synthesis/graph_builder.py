"""G-1 workflow unit graph builder (Epic #17 — Sub-Issue 3).

Reads collector output from ``github.db`` + ``sessions.db`` and writes a
deterministic graph (``graph_nodes`` + ``graph_edges``) into ``github.db``
so downstream sub-issues (unit identifier, metrics, weekly output) have a
single, stable substrate to traverse.

Node types
----------
* ``issue``    — one per ``issues`` row. ``node_id = "issue:{repo}#{number}"``
* ``pr``       — one per ``pull_requests`` row. ``node_id = "pr:{repo}#{number}"``
* ``commit``   — one per ``commits`` row. ``node_id = "commit:{sha}"``
* ``session``  — one per session row. ``node_id = "session:{uuid}"``

Edge types (src → dst)
----------------------
* ``pr_closes_issue`` — from ``pr_issues`` rows, plus ``link_resolver`` scan
  of each PR's ``head_ref`` + ``body``.
* ``pr_has_commit``   — every ``commits`` row with a ``pr_number`` contributes
  one edge from the owning PR to the commit.
* ``session_on_pr``   — from ``pr_sessions`` (collector) plus the same
  ``(branch, working_directory)`` predicate ``session_linker`` uses, so the
  graph can still be built when ``pr_sessions`` is sparse (Issue #36 note:
  ``pr_sessions`` has 0 live rows — expected).
* ``commit_refs_issue`` — ``#N`` scan of commit messages.
* ``timeline_ref``    — ``cross-referenced`` / ``referenced`` events link the
  originating issue to the referenced issue or PR.

Determinism
-----------
Nodes are written in ``(node_type, node_id)`` order; edges in
``(src_node_id, dst_node_id, edge_type)`` order. Writes use
``INSERT OR IGNORE`` so re-running against an already-populated ``github.db``
is a no-op. Running the builder twice in a row produces identical rows.

Sparse linkage
--------------
Sessions without any PR match become singleton nodes (no edges). This is
the *expected* shape for most Claude sessions today — ``pr_sessions`` is
empty in live data.

Offline
-------
No network calls, no LLM. ``sessions.db`` and ``github.db`` may point to
the same file (the Epic #17 golden fixture packs both schemas into one
SQLite file for convenience); the builder detects the overlap and avoids
attaching twice.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Union

from collector.github_poller.link_resolver import resolve_link

# ``#123`` references inside free text (commit messages, timeline payloads).
# Matches must start at a word boundary so raw shas/urls don't false-positive.
_HASH_REF_PATTERN = re.compile(r"(?:^|[^\w])#(\d+)\b")

# ``week_start`` value written for every node/edge when the caller did not
# supply one. The default is a stable literal so re-runs keep writing into
# the same partition (``INSERT OR IGNORE`` then makes the build idempotent).
# Downstream sub-issues that care about weekly bucketing must pass their
# own ``week_start``.
_ALL_WEEKS = "all"


# ---------------------------------------------------------------------------
# Public helpers (session matching predicate)
# ---------------------------------------------------------------------------


def session_matches_pr(
    session_branch: Optional[str],
    session_workdir: Optional[str],
    pr_head_ref: str,
    repo: str,
) -> bool:
    """Return True iff a session row links to a PR row.

    Factored out of ``session_linker.link_sessions`` so the graph builder
    and the collector speak the same matching predicate. Mirrors the
    collector's rule exactly: the session's ``git_branch`` must equal the
    PR's ``head_ref`` *and* the repo's short name (``owner/repo`` →
    ``repo``) must appear somewhere in the session's
    ``working_directory``. Either side being missing/empty is a miss.
    """
    if not session_branch or not pr_head_ref:
        return False
    if session_branch != pr_head_ref:
        return False
    repo_name = repo.split("/")[-1] if repo else ""
    if not repo_name:
        # No repo name means we cannot gate on working_directory — be
        # conservative and reject rather than risk cross-repo links.
        return False
    if repo_name not in (session_workdir or ""):
        return False
    return True


# ---------------------------------------------------------------------------
# Internal readers — one per source table. All take an open connection.
# ---------------------------------------------------------------------------


def _read_issues(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT repo, issue_number, created_at FROM issues ORDER BY repo, issue_number"
    ).fetchall()


def _read_prs(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT repo, pr_number, head_ref, body, created_at "
        "FROM pull_requests ORDER BY repo, pr_number"
    ).fetchall()


def _read_pr_issues(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT repo, pr_number, issue_number FROM pr_issues "
        "ORDER BY repo, pr_number, issue_number"
    ).fetchall()


def _read_pr_sessions(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT repo, pr_number, session_uuid FROM pr_sessions "
        "ORDER BY repo, pr_number, session_uuid"
    ).fetchall()


def _read_commits(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT repo, sha, pr_number, message, authored_at "
        "FROM commits ORDER BY repo, sha"
    ).fetchall()


def _read_timeline(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT repo, issue_number, event_id, event_type, payload_json, created_at "
        "FROM timeline_events ORDER BY repo, issue_number, event_id"
    ).fetchall()


def _read_sessions(
    conn: sqlite3.Connection,
    week_start: Optional[str],
) -> list[tuple]:
    """Return session rows, optionally filtered by activity in a week.

    The filter keeps any session whose [started_at, ended_at] interval
    overlaps the 7-day window beginning at ``week_start`` (YYYY-MM-DD).
    Sessions without a start timestamp are conservatively excluded when a
    filter is in effect — the epic ADR has ``session_started_at`` backfilled
    for historical rows, so missing timestamps mean "no evidence of activity"
    rather than "activity of unknown date".
    """
    rows = conn.execute(
        "SELECT session_uuid, git_branch, working_directory, "
        "session_started_at, session_ended_at "
        "FROM sessions ORDER BY session_uuid"
    ).fetchall()
    if not week_start:
        return rows
    # Compare as strings. ISO-8601 timestamps sort lexicographically, and
    # week_start is always YYYY-MM-DD, so ``started_at < week_end`` is a
    # valid prefix comparison.
    week_end = _add_days(week_start, 7)
    filtered: list[tuple] = []
    for uuid, branch, workdir, started_at, ended_at in rows:
        if not started_at:
            continue
        # Session overlaps the window iff it started before the week ended
        # and ended (or is still running, in which case we treat
        # ``ended_at`` as ``started_at``) on or after the week began.
        end = ended_at or started_at
        if started_at < week_end and end >= week_start:
            filtered.append((uuid, branch, workdir, started_at, ended_at))
    return filtered


def _add_days(ymd: str, days: int) -> str:
    """Return the YYYY-MM-DD string ``days`` days after ``ymd``.

    Kept as a tiny pure helper so the module has no ``datetime`` import
    leak outside this function. ``fromisoformat`` handles both bare dates
    and full timestamps; we slice to 10 chars so callers can pass either.
    """
    from datetime import date, timedelta

    base = date.fromisoformat(ymd[:10])
    return (base + timedelta(days=days)).isoformat()


# ---------------------------------------------------------------------------
# Node / edge ID helpers
# ---------------------------------------------------------------------------


def _issue_node(repo: str, number: int) -> str:
    return f"issue:{repo}#{number}"


def _pr_node(repo: str, number: int) -> str:
    return f"pr:{repo}#{number}"


def _commit_node(sha: str) -> str:
    return f"commit:{sha}"


def _session_node(uuid: str) -> str:
    return f"session:{uuid}"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def build_graph(
    sessions_db_path: Union[str, Path],
    github_db_path: Union[str, Path],
    week_start: Optional[str] = None,
) -> None:
    """Populate ``graph_nodes`` + ``graph_edges`` in ``github.db``.

    Parameters
    ----------
    sessions_db_path:
        Path to sessions.db. May equal ``github_db_path`` (the fixture
        case) — the builder opens each path only once.
    github_db_path:
        Path to github.db. All graph rows are written here.
    week_start:
        Optional YYYY-MM-DD string. When provided, only sessions active in
        that 7-day window are included. When ``None``, every session is
        included and rows are written under the sentinel ``"all"`` partition.

    Side effects
    ------------
    Writes to ``graph_nodes`` and ``graph_edges`` using ``INSERT OR IGNORE``.
    Safe to re-run: identical input → identical output.
    """
    gh_path = Path(github_db_path)
    sess_path = Path(sessions_db_path)
    partition = week_start if week_start else _ALL_WEEKS

    gh = sqlite3.connect(str(gh_path))
    sess = gh if sess_path == gh_path else sqlite3.connect(str(sess_path))
    try:
        issues = _read_issues(gh)
        prs = _read_prs(gh)
        pr_issues = _read_pr_issues(gh)
        pr_sessions = _read_pr_sessions(gh)
        commits = _read_commits(gh)
        timeline = _read_timeline(gh)
        sessions = _read_sessions(sess, week_start)

        nodes: dict[str, tuple[str, str, str]] = {}
        # ``nodes[node_id] = (node_type, node_ref, created_at)``.
        # Keyed by ``node_id`` so any re-hit is a no-op, which keeps the
        # build deterministic even if a later reader mentions the same ID.

        # --- nodes: issues / prs / commits -----------------------------
        for repo, number, created_at in issues:
            nid = _issue_node(repo, number)
            nodes.setdefault(nid, ("issue", f"{repo}#{number}", created_at or ""))

        pr_by_key: dict[tuple[str, int], tuple[str, str]] = {}
        # (repo, pr_number) -> (head_ref, body) — needed for the text-based
        # close-ref scan below.
        for repo, number, head_ref, body, created_at in prs:
            nid = _pr_node(repo, number)
            nodes.setdefault(nid, ("pr", f"{repo}#{number}", created_at or ""))
            pr_by_key[(repo, number)] = (head_ref or "", body or "")

        commit_by_sha: dict[str, tuple[str, Optional[int], str]] = {}
        for repo, sha, pr_number, message, authored_at in commits:
            nid = _commit_node(sha)
            nodes.setdefault(nid, ("commit", f"{repo}@{sha}", authored_at or ""))
            commit_by_sha[sha] = (repo, pr_number, message or "")

        # --- nodes: sessions -------------------------------------------
        # Build a branch-indexed lookup so the session_on_pr predicate
        # runs in O(PRs + sessions) rather than O(PRs * sessions).
        sessions_by_branch: dict[str, list[tuple[str, str]]] = {}
        for uuid, branch, workdir, started_at, _ended_at in sessions:
            nid = _session_node(uuid)
            nodes.setdefault(nid, ("session", uuid, started_at or ""))
            if branch:
                sessions_by_branch.setdefault(branch, []).append(
                    (uuid, workdir or "")
                )

        # --- edges -----------------------------------------------------
        # (src_node_id, dst_node_id, edge_type) — set-dedup, then sorted.
        edges: set[tuple[str, str, str]] = set()

        # pr_closes_issue (from pr_issues linkage table)
        for repo, pr_number, issue_number in pr_issues:
            edges.add(
                (
                    _pr_node(repo, pr_number),
                    _issue_node(repo, issue_number),
                    "pr_closes_issue",
                )
            )

        # pr_closes_issue (text-based; re-use link_resolver so branch/body
        # parsing stays in one place)
        for (repo, pr_number), (head_ref, body) in pr_by_key.items():
            resolved = resolve_link(head_ref, body)
            if resolved is None:
                continue
            issue_nid = _issue_node(repo, resolved)
            # Only add the edge when the referenced issue actually exists
            # in the graph — a free-text "#N" reference to a non-existent
            # issue would otherwise dangle.
            if issue_nid in nodes:
                edges.add(
                    (_pr_node(repo, pr_number), issue_nid, "pr_closes_issue")
                )

        # pr_has_commit
        for sha, (repo, pr_number, _message) in commit_by_sha.items():
            if pr_number is None:
                continue
            pr_nid = _pr_node(repo, pr_number)
            if pr_nid not in nodes:
                # Commit claims a PR number we don't have a row for;
                # surface the commit as a standalone node but don't invent
                # a dangling edge.
                continue
            edges.add((pr_nid, _commit_node(sha), "pr_has_commit"))

        # session_on_pr (from the pr_sessions linkage table)
        session_nids = {nid for nid, (t, *_rest) in nodes.items() if t == "session"}
        for repo, pr_number, session_uuid in pr_sessions:
            sess_nid = _session_node(session_uuid)
            if sess_nid not in session_nids:
                # Session was filtered out by week_start — skip the edge
                # rather than materialise a node outside the requested
                # window.
                continue
            edges.add(
                (
                    sess_nid,
                    _pr_node(repo, pr_number),
                    "session_on_pr",
                )
            )

        # session_on_pr (fallback: same predicate session_linker uses, so
        # sparse pr_sessions data still produces useful edges). ``head_ref``
        # is the PR's branch; the matching session's branch is also
        # ``head_ref`` by virtue of being keyed in ``sessions_by_branch``.
        for (repo, pr_number), (head_ref, _body) in pr_by_key.items():
            if not head_ref:
                continue
            for uuid, workdir in sessions_by_branch.get(head_ref, []):
                if not session_matches_pr(
                    session_branch=head_ref,
                    session_workdir=workdir,
                    pr_head_ref=head_ref,
                    repo=repo,
                ):
                    continue
                edges.add(
                    (
                        _session_node(uuid),
                        _pr_node(repo, pr_number),
                        "session_on_pr",
                    )
                )

        # commit_refs_issue (#N scan of commit messages)
        for sha, (repo, _pr_number, message) in commit_by_sha.items():
            for issue_number in _extract_hash_refs(message):
                issue_nid = _issue_node(repo, issue_number)
                if issue_nid not in nodes:
                    continue
                edges.add(
                    (_commit_node(sha), issue_nid, "commit_refs_issue")
                )

        # timeline_ref (cross_referenced / referenced events)
        for repo, issue_number, _event_id, event_type, payload_json, _created_at in timeline:
            if event_type not in ("cross-referenced", "cross_referenced", "referenced"):
                continue
            # Payload encodes the source issue/PR that references us. The
            # GitHub REST shape nests it under ``source.issue.number``; we
            # tolerate either that or a flat ``{"number": N, "type": "..."}``
            # shape for synthesised payloads.
            referenced_number, referenced_type = _extract_timeline_target(payload_json)
            if referenced_number is None:
                continue
            src_nid = _issue_node(repo, issue_number)
            if src_nid not in nodes:
                continue
            if referenced_type == "pull_request":
                dst_nid = _pr_node(repo, referenced_number)
            else:
                dst_nid = _issue_node(repo, referenced_number)
            if dst_nid not in nodes:
                continue
            edges.add((src_nid, dst_nid, "timeline_ref"))

        # --- write -----------------------------------------------------
        sorted_nodes = sorted(nodes.items(), key=lambda kv: (kv[1][0], kv[0]))
        sorted_edges = sorted(edges)

        for node_id, (node_type, node_ref, created_at) in sorted_nodes:
            gh.execute(
                "INSERT OR IGNORE INTO graph_nodes "
                "(week_start, node_id, node_type, node_ref, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (partition, node_id, node_type, node_ref, created_at),
            )
        for src, dst, etype in sorted_edges:
            gh.execute(
                "INSERT OR IGNORE INTO graph_edges "
                "(week_start, src_node_id, dst_node_id, edge_type) "
                "VALUES (?, ?, ?, ?)",
                (partition, src, dst, etype),
            )
        gh.commit()
    finally:
        if sess is not gh:
            sess.close()
        gh.close()


# ---------------------------------------------------------------------------
# Text-extraction helpers
# ---------------------------------------------------------------------------


def _extract_hash_refs(text: str) -> list[int]:
    """Return unique ``#N`` issue numbers referenced in ``text``, in order."""
    if not text:
        return []
    seen: set[int] = set()
    out: list[int] = []
    for match in _HASH_REF_PATTERN.finditer(text):
        n = int(match.group(1))
        if n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _extract_timeline_target(payload_json: Optional[str]) -> tuple[Optional[int], Optional[str]]:
    """Return ``(number, kind)`` for a timeline reference payload.

    ``kind`` is ``"pull_request"`` when the payload marks the referenced
    item as a PR, ``"issue"`` otherwise. Returns ``(None, None)`` when
    nothing extractable is present.
    """
    if not payload_json:
        return None, None
    try:
        payload = json.loads(payload_json)
    except (ValueError, TypeError):
        return None, None
    if not isinstance(payload, dict):
        return None, None
    # GitHub's nested shape
    source = payload.get("source")
    if isinstance(source, dict):
        issue = source.get("issue")
        if isinstance(issue, dict) and isinstance(issue.get("number"), int):
            kind = "pull_request" if issue.get("pull_request") else "issue"
            return issue["number"], kind
    # Flat shape (synthesised / abbreviated payloads)
    number = payload.get("number")
    if isinstance(number, int):
        kind = payload.get("type") or "issue"
        return number, kind
    return None, None
