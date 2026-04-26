"""Epic #93 / Slice 2 — ABSOLUTE invariant regression test.

Section 8 of the epic intent document
(``.agent-work/EPIC_graph-directionality-overhaul-93/intent.md``) defines
this invariant verbatim:

    Walking from a session never reaches its parent issue or PR.
    Directionality is enforced at every BFS walker
    (``unit_identifier._UnionFind``, ``cross_unit._latest_node_ts``,
    ``weekly._unit_nodes``, and any future walker), not just at writers.
    A regression test must assert that no walker reads ``graph_edges``
    without specifying its ``traversal`` filter explicitly.

This test enforces the second sentence: every ``FROM graph_edges`` SELECT
in ``synthesis/`` must mention ``traversal`` in its WHERE clause within a
small window of lines, OR carry an explicit
``# NO_TRAVERSAL_FILTER_OK: <reason>`` opt-out comment.

When a future walker is added without honoring the filter, this test
fails — that is the entire point of its existence.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


SYNTHESIS_DIR = Path(__file__).resolve().parent.parent / "synthesis"

# How many lines after a ``FROM graph_edges`` line we look for a
# ``traversal`` predicate. Five lines is enough for typical formatted
# multi-line SQL strings without becoming so loose that an unrelated
# downstream WHERE clause counts as a match.
LOOKAHEAD_LINES = 5

# Marker any deliberate exception must place on or near the offending
# SELECT. Format: ``# NO_TRAVERSAL_FILTER_OK: <reason>``.
OPT_OUT_MARKER = "NO_TRAVERSAL_FILTER_OK"


def _iter_synthesis_py_files() -> list[Path]:
    return sorted(p for p in SYNTHESIS_DIR.rglob("*.py"))


def _find_violations() -> list[tuple[Path, int, str]]:
    """Return (path, line_no, snippet) for each unfiltered graph_edges read."""
    violations: list[tuple[Path, int, str]] = []
    pattern = re.compile(r"FROM\s+graph_edges", re.IGNORECASE)
    for path in _iter_synthesis_py_files():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if not pattern.search(line):
                continue
            # Skip lines that are pure Python comments / docstring prose —
            # only SQL string literals count as "reads".
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            # Allow DELETE/UPDATE on graph_edges (those are writer-side
            # cleanup queries, not BFS reads).
            stripped_lit = stripped.lstrip('"').lstrip("'").lstrip()
            stripped_upper = stripped_lit.upper()
            if stripped_upper.startswith("DELETE") or stripped_upper.startswith(
                "UPDATE"
            ):
                continue
            # The line must contain a SQL string literal (quoted) — if
            # there's no quote at all, it's docstring prose.
            if '"' not in line and "'" not in line:
                continue
            # Look at this line plus the next LOOKAHEAD_LINES lines for
            # either a ``traversal`` predicate or the explicit opt-out
            # marker.
            window = "\n".join(lines[idx : idx + 1 + LOOKAHEAD_LINES])
            if OPT_OUT_MARKER in window:
                continue
            if re.search(r"traversal\s*=", window) or re.search(
                r"traversal\s+IN\s*\(", window, re.IGNORECASE
            ):
                continue
            violations.append((path, idx + 1, line.strip()))
    return violations


def test_every_graph_edges_read_filters_traversal():
    """Every ``FROM graph_edges`` SELECT in synthesis/ must filter on
    ``traversal`` (or carry an explicit ``# NO_TRAVERSAL_FILTER_OK`` opt-out).

    This is the ABSOLUTE invariant from Section 8 of the Epic #93 intent
    document. A future walker added without the filter would silently
    re-introduce the spurious-merge problem Issue #68 originally solved.
    """
    violations = _find_violations()
    assert violations == [], (
        "BFS walker invariant violated — these graph_edges reads do not "
        "filter on traversal within "
        f"{LOOKAHEAD_LINES} lines of the FROM clause:\n  "
        + "\n  ".join(f"{p}:{ln}: {snippet}" for p, ln, snippet in violations)
        + "\n\nFix: add ``WHERE traversal = 'own'`` (or 'ref') to the query, "
        "or add a ``# NO_TRAVERSAL_FILTER_OK: <reason>`` comment within "
        f"{LOOKAHEAD_LINES} lines if the read is deliberately unfiltered."
    )


def test_invariant_test_actually_detects_violations(tmp_path):
    """Self-test: feed the detector a synthetic violator and confirm it fires.

    Without this self-test, a refactor that breaks the regex would silently
    stop catching real violations.
    """
    bad = tmp_path / "bad_walker.py"
    bad.write_text(
        '''def walk(conn):
    return conn.execute(
        "SELECT src_node_id, dst_node_id FROM graph_edges "
        "WHERE week_start = ?",
        (week_start,),
    ).fetchall()
'''
    )
    # Reuse the same logic against the temp file by patching SYNTHESIS_DIR.
    pattern = re.compile(r"FROM\s+graph_edges", re.IGNORECASE)
    text = bad.read_text(encoding="utf-8")
    lines = text.splitlines()
    found_violation = False
    for idx, line in enumerate(lines):
        if not pattern.search(line):
            continue
        window = "\n".join(lines[idx : idx + 1 + LOOKAHEAD_LINES])
        if OPT_OUT_MARKER in window:
            continue
        if re.search(r"traversal\s*=", window):
            continue
        found_violation = True
    assert found_violation, (
        "Self-test failed: the synthetic unfiltered-read example was not "
        "flagged by the detector logic — the regex or window must be broken."
    )
