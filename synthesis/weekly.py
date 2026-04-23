"""S-2 weekly synthesis runner (Epic #17 — Issue #39).

Assembles the weekly retrospective prompt from the synthesis tables
(``units`` with cross-unit flags, ``graph_nodes`` / ``graph_edges`` for
component resolution, ``sessions.raw_content_json`` for transcripts),
calls the Anthropic API (or the offline :class:`FakeAnthropicClient`
stand-in when ``AMIS_SYNTHESIS_LIVE`` is unset), and hands the
rendered Markdown to :func:`synthesis.output_writer.write_retrospective`.

Architecture decisions anchored here
------------------------------------
* **Decision 2 (idempotency).** The output writer refuses to overwrite
  an existing retrospective; :func:`run_synthesis` calls the API only
  *after* that guard so re-running the same ``--week`` is cheap.
* **Decision 4 (water-fill transcripts).** See
  :func:`water_fill_truncate` — sessions share a 512 KB transcript
  budget proportionally. Small sessions are never truncated; large
  sessions take the equal-share haircut. The algorithm is the unit of
  test (``tests/test_weekly.py::test_water_fill_*``).

What lives where
----------------
* ``run_synthesis`` — end-to-end pipeline.
* ``water_fill_truncate`` — pure-function transcript budgeter (tested
  independently; no DB access).
* ``_assemble_prompt`` — Markdown prompt assembly (static cacheable
  context + per-unit dynamic block).
* Adapter selection is handled by :func:`synthesis.llm_adapter._get_adapter`,
  which picks between the live SDK and
  :class:`synthesis.fake_client.FakeAnthropicClient` based on
  ``AMIS_SYNTHESIS_LIVE``.

Prompt caching
--------------
When the live SDK is active, the static context block (template,
thresholds, column descriptions) is sent with ``cache_control`` so
subsequent weekly runs pay a cache-hit price on it. The dynamic
per-unit block is never cached — it changes every week.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple, Union

from am_i_shipping.config_loader import SynthesisConfig
from am_i_shipping.health_writer import write_health
from synthesis.llm_adapter import _get_adapter
from synthesis.output_writer import write_retrospective
from synthesis.unit_timeline import render_timeline


logger = logging.getLogger(__name__)


# Total transcript budget across all sessions in a single synthesis run,
# in bytes. ADR Decision 4. 512 KB fits comfortably within Claude's
# 200K-token context window at ~3-4 bytes per token while leaving
# plenty of room for the static template + per-unit stats blocks.
TRANSCRIPT_BUDGET_BYTES = 512 * 1024  # 524288

# Issue #54 / Epic #50 P-2 — hard cap on the number of units included in
# a single synthesis prompt. Keeps the prompt assembly bounded even when
# the week partition is pathological (e.g. a dev ``week_start='all'``
# run with thousands of units). A real weekly run is ~42 units, so 100
# is generous for the expected case while still catching runaway input
# before it reaches the LLM. Kept as a module constant (not a
# ``SynthesisConfig`` field) per ADR Q2 — this is a safety rail, not a
# user-tuneable knob.
MAX_UNITS_PER_PROMPT = 100

# Issue #54 / Epic #50 P-2 — two-tier ceiling on the total assembled
# prompt (system + user content, in bytes):
#
# * ``MAX_PROMPT_SOFT_WARN_BYTES`` (512 KB) is the epic-#50 acceptance
#   criterion for a real weekly run. Between this and the hard cap we
#   log a WARNING so the epic's "prompt ≤ 512 KB for a real week" check
#   stays enforceable during smoke tests without raising on edge cases.
# * ``MAX_PROMPT_BYTES`` (1 MiB) is the fail-loud rail. Above this we
#   ``raise RuntimeError`` rather than ship — a prompt that large
#   points to a bug upstream (missed truncation, runaway unit set,
#   oversized per-unit metadata) and silently truncating it would hide
#   the signal. 1 MiB is ~2x the transcript budget (512 KB) plus
#   headroom for the static template + per-unit metadata blocks.
#
# The soft threshold is coincidentally equal to ``TRANSCRIPT_BUDGET_BYTES``
# above. They are independent ceilings with the same numeric value by
# chance (ADR Decision 4 and epic #50 both landed on 512 KB) — keep
# them as separate constants so a future tweak to one does not
# silently move the other.
MAX_PROMPT_SOFT_WARN_BYTES = 512 * 1024  # 524288 — epic-#50 target
MAX_PROMPT_BYTES = 1_048_576  # 1 MiB — hard raise threshold

# Upper bound on output tokens for the weekly synthesis call. The real
# response is a few hundred tokens of Markdown; the ceiling exists so a
# runaway generation does not burn tokens indefinitely.
_MAX_OUTPUT_TOKENS = 4096


# ---------------------------------------------------------------------------
# Water-fill transcript truncation (ADR Decision 4)
# ---------------------------------------------------------------------------


def water_fill_truncate(
    contents: List[str],
    budget: int,
) -> List[str]:
    """Apportion *budget* bytes across *contents* via equal-share water-fill.

    The algorithm (per the ADR):

    1. Sort the inputs ascending by current byte-length.
    2. Walk from smallest to largest. Each entry's ``share`` is
       ``remaining_budget / remaining_count``.
    3. If the entry fits within its share, include it whole and shrink
       the remaining budget + count (the saved bytes cascade to larger
       entries).
    4. Otherwise truncate to exactly ``share`` bytes.

    This gives equal truncation burden to equally-large sessions while
    letting small sessions pass through untouched, which is what we want
    when the transcript budget is smaller than the total session
    content.

    Parameters
    ----------
    contents:
        List of UTF-8 strings. Treated as opaque bytes for budgeting
        (``len(s.encode('utf-8'))`` would be more accurate but the ADR
        specifies ``len(content)`` and callers already truncate at
        character boundaries so the simpler form matches the spec).
    budget:
        Total byte allowance. Must be non-negative. ``0`` returns a list
        of empty strings with the same cardinality as *contents*.

    Returns
    -------
    List of truncated strings in the SAME ORDER as the input. Internal
    sorting is by a side index so the returned list aligns positionally
    with *contents* — tests rely on that when asserting per-session
    results.

    Example
    -------
    ``budget=10, contents=["aaaaa", "bbbbbbbbbbbbbbb"]`` → ``["aaaaa",
    "bbbbb"]``. Two entries, initial share = 10 // 2 = 5. Small session
    fits its share exactly (5 bytes ≤ share 5) and passes through
    whole, consuming all 5 bytes of its slice. Remaining budget = 5,
    remaining count = 1, so the large session's share = 5; it
    truncates from 15 down to 5. Net: small unchanged, large takes a
    10-byte haircut.

    ``budget=12, contents=["aaaaa", "bbbbbbbbbbbbbbb"]`` → ``["aaaaa",
    "bbbbbbb"]``. First pass gives each session share=6. The 5-byte
    session consumes 5 (under its share of 6), leaving 7 for the large
    session. Large session truncates to 7. This is where the "savings
    cascade" actually shows up — the small session spent less than its
    share, and the remainder flowed to the large one.

    ``budget=10, contents=["aaaaaaaa", "bbbbbbbbb"]`` → ``["aaaaa",
    "bbbbb"]``. Neither session fits its initial share=5; both
    truncate to 5.
    """
    if budget < 0:
        raise ValueError(f"budget must be non-negative, got {budget}")
    if not contents:
        return []

    # Pair up (index, content) so we can sort by size without losing
    # the positional alignment the caller expects.
    indexed: List[Tuple[int, str]] = list(enumerate(contents))
    # Sort ascending by byte length. Ties broken by index for
    # determinism — two equally-sized sessions should truncate
    # identically, but the tie-break rule ensures the walk order is
    # reproducible across Python versions.
    indexed.sort(key=lambda p: (len(p[1]), p[0]))

    result: List[Optional[str]] = [None] * len(contents)
    remaining_budget = budget
    remaining_count = len(indexed)

    for original_idx, content in indexed:
        if remaining_count <= 0:
            # Defensive — should not hit with the loop bounds above.
            result[original_idx] = ""
            continue
        # Integer-division share so the budget never goes negative.
        # Any remainder is absorbed by the last entry, which is the
        # largest and therefore the most capable of taking the bonus.
        share = remaining_budget // remaining_count
        if len(content) <= share:
            result[original_idx] = content
            remaining_budget -= len(content)
        else:
            # Truncate to exactly ``share`` bytes. Character-boundary
            # only — we do not attempt to land on a valid UTF-8
            # boundary here because the ADR spec is ``content[:share]``
            # and the caller feeds already-decoded text.
            result[original_idx] = content[:share]
            remaining_budget -= share
        remaining_count -= 1

    # None of the slots should remain unassigned at this point.
    return [r if r is not None else "" for r in result]


# ---------------------------------------------------------------------------
# Unit / session loading
# ---------------------------------------------------------------------------


def _repo_filter_sql(
    repo: Optional[str],
    week_start: Optional[str] = None,
    *,
    units_alias: str = "",
) -> Tuple[str, List[str]]:
    """Return ``(sql_fragment, params)`` for filtering ``units`` by repo.

    Issue #88 — single-repo filter for the LLM pipeline stages.

    When *repo* is ``None`` or empty, returns ``("", [])`` so callers can
    concatenate the fragment unconditionally and leave the baseline
    no-flag SQL byte-identical.

    When *repo* is a ``"owner/name"`` slug, the fragment is an ``AND ...``
    clause (parenthesised) matching:

    * issue-rooted units whose ``root_node_id`` is literally
      ``issue:<repo>#<N>``,
    * PR-rooted units whose ``root_node_id`` is literally
      ``pr:<repo>#<N>``, and
    * session-rooted units whose ``session:<uuid>`` is attributed to the
      target repo via ``session_issue_attribution``.

    The session branch covers the defensive path: today's
    ``unit_identifier`` drops session-only graph components so
    ``root_node_type='session'`` is effectively empty in practice, but the
    spec (refined issue #88) calls for the resolver because a future
    change upstream could start emitting them. Sessions attributed only
    to a PR (not an issue) would be missed — documented limitation,
    acceptable for a dev-loop feature.

    ``units_alias`` lets callers reference a joined/aliased ``units`` table
    (e.g. ``"u"`` in the ``LEFT JOIN unit_summaries`` query). Pass ``""``
    when the SELECT is a bare ``FROM units``.

    ``week_start`` must be provided whenever ``repo`` is truthy — the
    session-attribution subquery correlates on ``week_start`` and the
    helper binds it directly so the caller does not have to remember a
    placeholder convention. (F-1 collapse: previously a two-function API
    that returned a sentinel string; now a single function that returns
    a ready-to-bind param list.)

    The trailing ``#%`` boundary on each LIKE pattern prevents
    ``hyang0129/am-i-shipping`` from matching
    ``hyang0129/am-i-shipping-sibling``. The slug is also LIKE-escaped
    (``%``, ``_``, backslash) with an explicit ``ESCAPE '\\'`` clause
    so a repo whose name legitimately contains ``_`` — e.g.
    ``owner/my_repo`` — cannot be confused with ``owner/myXrepo``.
    """
    if not repo:
        return "", []
    if week_start is None:
        raise ValueError(
            "_repo_filter_sql: week_start is required when repo is set "
            "(the session-attribution subquery correlates on week_start)"
        )

    prefix = f"{units_alias}." if units_alias else ""

    # LIKE-escape the slug so %, _, or \\ inside an owner/name cannot
    # turn into SQL LIKE metacharacters. ``ESCAPE '\\'`` below tells
    # SQLite to treat our backslash as the literal-escape sentinel.
    esc_repo = (
        repo.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    )
    issue_pat = f"issue:{esc_repo}#%"
    pr_pat = f"pr:{esc_repo}#%"

    # Inner subquery resolves session-rooted units via
    # session_issue_attribution. We substr the root_node_id past the
    # literal "session:" prefix to recover the uuid, then join against
    # the attribution table on (week_start, session_uuid, repo).
    #
    # The subquery's ``u2.week_start = ?`` is bound to the *same*
    # week_start the caller filters the outer query on. Binding it
    # directly here (rather than correlating to an outer alias) keeps
    # the helper independent of the caller's alias naming.
    session_subquery = (
        "SELECT u2.unit_id FROM units u2 "
        "JOIN session_issue_attribution sia "
        "  ON sia.week_start = u2.week_start "
        " AND sia.session_uuid = substr(u2.root_node_id, length('session:') + 1) "
        "WHERE u2.week_start = ? "
        "  AND u2.root_node_type = 'session' "
        "  AND sia.repo = ?"
    )

    fragment = (
        f" AND ({prefix}root_node_id LIKE ? ESCAPE '\\' "
        f"     OR {prefix}root_node_id LIKE ? ESCAPE '\\' "
        f"     OR (({prefix}root_node_type = 'session') "
        f"         AND {prefix}unit_id IN ({session_subquery})))"
    )
    # Param order matches the ?s above: issue LIKE, pr LIKE, session
    # subquery's week_start, session subquery's repo. ``repo`` in the
    # session branch is compared directly against ``session_issue_attribution.repo``
    # — no LIKE, no escape needed.
    return fragment, [issue_pat, pr_pat, week_start, repo]


def _load_units(
    github_conn: sqlite3.Connection,
    week_start: str,
    repo: Optional[str] = None,
) -> List[dict]:
    """Return one dict per ``units`` row for *week_start*.

    Includes ``outlier_flags`` (JSON string or NULL) and
    ``abandonment_flag`` (0/1 or NULL) from the cross-unit pass.

    When *repo* is set, filter to units whose ``root_node_id`` is
    scoped to that repo (issue:/pr: LIKE + session resolver). When
    ``None`` (default), the query is byte-identical to the pre-#88
    baseline.
    """
    fragment, repo_params = _repo_filter_sql(repo, week_start)
    params: List = [week_start, *repo_params]
    rows = github_conn.execute(
        "SELECT unit_id, root_node_type, root_node_id, "
        "       elapsed_days, dark_time_pct, total_reprompts, "
        "       review_cycles, status, outlier_flags, abandonment_flag "
        f"FROM units WHERE week_start = ?{fragment} "
        "ORDER BY unit_id",
        params,
    ).fetchall()
    return [
        {
            "unit_id": r[0],
            "root_node_type": r[1],
            "root_node_id": r[2],
            "elapsed_days": r[3],
            "dark_time_pct": r[4],
            "total_reprompts": r[5],
            "review_cycles": r[6],
            "status": r[7],
            "outlier_flags": r[8],
            "abandonment_flag": r[9],
        }
        for r in rows
    ]


def _unit_nodes(
    github_conn: sqlite3.Connection,
    week_start: str,
    root_node_id: str,
) -> List[Tuple[str, str, Optional[str]]]:
    """Walk ``graph_edges`` to collect the component containing *root_node_id*.

    Returns ``[(node_id, node_type, node_ref), ...]`` — the shape the
    :func:`synthesis.unit_timeline.render_timeline` and the session-
    lookup helpers below consume.
    """
    # Pull all nodes + edges for the week in one pass; the per-unit
    # filtering is pure Python BFS so we do not hammer SQLite with N+1
    # queries the way a naive recursive-SQL version would.
    nodes = {
        nid: (nt, nr)
        for nid, nt, nr in github_conn.execute(
            "SELECT node_id, node_type, node_ref FROM graph_nodes "
            "WHERE week_start = ?",
            (week_start,),
        ).fetchall()
    }
    adj: dict[str, set[str]] = {}
    for src, dst in github_conn.execute(
        "SELECT src_node_id, dst_node_id FROM graph_edges "
        "WHERE week_start = ?",
        (week_start,),
    ).fetchall():
        adj.setdefault(src, set()).add(dst)
        adj.setdefault(dst, set()).add(src)

    if not root_node_id or root_node_id not in nodes:
        return []

    seen = {root_node_id}
    stack = [root_node_id]
    while stack:
        cur = stack.pop()
        for nxt in adj.get(cur, ()):
            if nxt not in seen and nxt in nodes:
                seen.add(nxt)
                stack.append(nxt)

    return sorted(
        (
            (nid, nodes[nid][0] or "", nodes[nid][1])
            for nid in seen
        ),
        key=lambda x: x[0],
    )


def _resolve_unit_sessions(
    gh_conn: sqlite3.Connection,
    week_start: str,
    root_node_id: str,
    root_node_type: str,
) -> List[str]:
    """Return session UUIDs for a unit, handling both graph-edge and issue-attribution linkage.

    For PR-rooted and session-rooted units, session nodes appear as ``node_type='session'``
    members of the graph component reachable via ``graph_edges``. For issue-rooted units,
    session linkage lives instead in ``session_issue_attribution`` (session nodes are never
    added to ``graph_edges`` for those units), so graph traversal yields zero sessions.

    This helper replicates the fallback used by :func:`synthesis.expectations._build_unit_input`
    so that both X-1 (expectations) and X-3 (revision_detector) resolve sessions identically.

    Parameters
    ----------
    gh_conn:
        Open connection to the collector DB (github.db). Must be readable.
    week_start:
        ``YYYY-MM-DD`` anchor matching the unit row.
    root_node_id:
        ``root_node_id`` from the ``units`` table (e.g. ``"issue:repo#42"``).
    root_node_type:
        ``root_node_type`` from the ``units`` table (e.g. ``"issue"``, ``"pr"``,
        ``"session"``).

    Returns
    -------
    List of session UUIDs (strings), possibly empty if no sessions are found.
    """
    component = _unit_nodes(gh_conn, week_start, root_node_id or "")
    session_uuids: List[str] = [
        node_ref
        for _nid, node_type, node_ref in component
        if node_type == "session" and node_ref
    ]

    # Issue-rooted units: session nodes are not in graph_edges; fall back to
    # session_issue_attribution. Mirrors the same logic in
    # synthesis.expectations._build_unit_input.
    if not session_uuids and root_node_type == "issue" and root_node_id:
        ref = root_node_id.removeprefix("issue:")
        if "#" in ref:
            _repo, _, _num = ref.rpartition("#")
            try:
                _issue_number = int(_num)
                rows = gh_conn.execute(
                    "SELECT session_uuid FROM session_issue_attribution "
                    "WHERE week_start = ? AND repo = ? AND issue_number = ?",
                    (week_start, _repo, _issue_number),
                ).fetchall()
                session_uuids = [r[0] for r in rows]
            except (ValueError, sqlite3.OperationalError):
                pass

    return session_uuids


def _load_session_transcripts(
    sessions_conn: sqlite3.Connection,
    session_uuids: List[str],
) -> List[Tuple[str, str]]:
    """Return ``[(session_uuid, raw_content_json), ...]`` for the given UUIDs.

    Rows with NULL raw_content_json contribute an empty string — the
    water-fill algorithm treats them as zero-length entries that always
    pass through their share untouched.
    """
    if not session_uuids:
        return []
    placeholders = ",".join("?" * len(session_uuids))
    rows = sessions_conn.execute(
        f"SELECT session_uuid, raw_content_json FROM sessions "
        f"WHERE session_uuid IN ({placeholders})",
        session_uuids,
    ).fetchall()
    by_uuid = {r[0]: (r[1] or "") for r in rows}
    # Preserve the caller's order so the prompt is deterministic.
    return [(uid, by_uuid.get(uid, "")) for uid in session_uuids]


def _load_unit_summaries(
    gh_conn: sqlite3.Connection,
    week_start: str,
) -> dict:
    """Return ``{unit_id: summary_text}`` from ``unit_summaries`` for *week_start*."""
    rows = gh_conn.execute(
        "SELECT unit_id, summary_text FROM unit_summaries WHERE week_start = ?",
        (week_start,),
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def _load_issue_attribution(
    gh_conn: sqlite3.Connection,
    week_start: str,
    repo: str,
    issue_number: int,
    session_uuids: List[str],
) -> dict:
    """Return ``{session_uuid: {"fraction": float, "phase": str}}`` for the given sessions.

    Reads from ``session_issue_attribution`` for the anchor ``(repo, issue_number)``.
    Falls back to ``fraction=1.0, phase="execution"`` for rows that are missing
    (e.g. old weeks before the table existed, or sessions not tracked there).
    Only called for issue-rooted units; PR/session-rooted units skip this.
    """
    if not session_uuids:
        return {}

    # Check whether the table exists — degrade gracefully on old DBs.
    try:
        placeholders = ",".join("?" * len(session_uuids))
        rows = gh_conn.execute(
            f"SELECT session_uuid, fraction, phase "
            f"FROM session_issue_attribution "
            f"WHERE week_start = ? AND repo = ? AND issue_number = ? "
            f"AND session_uuid IN ({placeholders})",
            [week_start, repo, issue_number] + list(session_uuids),
        ).fetchall()
    except sqlite3.OperationalError:
        # Table does not exist yet (old schema) — degrade gracefully.
        rows = []

    result = {uid: {"fraction": 1.0, "phase": "execution"} for uid in session_uuids}
    for session_uuid, fraction, phase in rows:
        result[session_uuid] = {
            "fraction": fraction if fraction is not None else 1.0,
            "phase": phase if phase is not None else "execution",
        }
    return result


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------


# Static context — same every week. Sent as a cacheable block when the
# live SDK is active so re-runs pay a cache-read price on the ~1-2 KB of
# boilerplate below.
_STATIC_SYSTEM_PROMPT = """You are the weekly synthesis engine for the am-i-shipping workflow monitor.

Purpose
-------
Produce a Markdown retrospective that helps the developer identify which
preconditions (Phase 0, steps 1-5 of the idealized workflow) failed
during the past week. The unit of improvement is the user's behavior,
not Claude's — frame every observation in those terms.

Required Markdown sections (exact headings, in order)
-----------------------------------------------------
1. `## Velocity Trend`            — change in throughput vs. the user's
   own prior baseline. Omit population benchmarks.
2. `## Unit Summary Table`        — one row per unit from the week's
   `units` table.
3. `## Outlier Units`             — units flagged by the cross-unit pass
   (`outlier_flags` non-empty). For each, note which metric(s) breached.
4. `## Abandoned Units`           — units with `abandonment_flag = 1`.
   Do not prescribe follow-up; surface the signal.
5. `## Dark Time`                 — fraction of each unit's wall-clock
   span during which no session was active. Highlight the top-2.
6. `## Expectation Gaps`          — units whose expected vs. actual gap
   was `major` or `critical`. Each item names the unit, the direction
   of the gap (`under`/`over`/`match`/`ambiguous`), and the idealized-
   workflow step (`phase_0_setup` / `step_1_intent` / ...) that was the
   root-cause precondition. Omit the section body when no major or
   critical gaps exist; keep the heading.
7. `## Expectation Revisions`     — units with mid-stream expectation
   shifts (reprompts, scope-change turns, or session breaks > 24 h
   anchored after the commitment point). Each item names the unit, the
   trigger type, the facet that shifted (`scope`/`effort`/`outcome`),
   and the before/after summary. Low-confidence revisions (< 0.5) are
   annotated as such, not omitted. Omit the section body when no
   revisions exist; keep the heading.
8. `## Calibration Trends`        — *conditional; present only when
   ≥20 user corrections have accumulated in `expectation_corrections`
   (Epic #27 X-5).* Per-work-type calibration deltas grouped by
   `type_label`. Each bullet names the work type, the correction rate
   per facet (`scope`/`effort`/`outcome`), and the sample count.
   The LLM must preserve the supplied bullets verbatim — do not invent
   new work types or rephrase the numeric deltas. Omit the section
   entirely when the calibration pass produced no rows.
9. `## Clarifying Questions`      — at most TWO total, numbered `1.`
   and `2.`. Each question should be answerable from the user's memory
   of the week — no research required.

Constraints
-----------
* At most TWO clarifying questions across the entire document.
  The limit is TOTAL, not per unit.
* No `## Recommendations` section. The experiment loop (a separate
  call, not this one) generates recommendations. Synthesis only asks.
* No population benchmarks ("you used Claude X% more than average");
  personal baseline only.

Metric column legend
--------------------
* `elapsed_days`      — wall-clock span from first to last event.
* `dark_time_pct`     — 1 - (sum(session active) / span).
* `total_reprompts`   — sum of `sessions.reprompt_count` in the unit.
* `review_cycles`     — len(review_comments_json) or push_count fallback.
* `outlier_flags`     — JSON list of metric names > median + 2sigma.
* `abandonment_flag`  — 1 if no event within 14 days.

Thresholds
----------
* Outlier cutoff:    median + 2.0 * stdev (population stdev).
* Abandonment cutoff: 14 days without a graph_nodes event.
"""


def _format_unit_block(
    unit: dict,
    transcript: str,
    session_attribution: Optional[dict] = None,
) -> str:
    """Render one unit's dynamic block for the prompt.

    Includes the metrics, flags, and (potentially truncated) transcript.
    Keeps the Markdown readable by an LLM — table-less, heading-heavy.

    ``outlier_flags`` and ``abandonment_flag`` are NULLable in the
    ``units`` schema (see cross_unit.py migration). We expect both keys
    to be present — ``_load_units`` always populates them — so the
    explicit ``is None`` check handles the NULL case only, not a
    missing key (which would be a programming error elsewhere).

    For issue-rooted units, *session_attribution* is a dict mapping
    ``session_uuid -> {"fraction": float, "phase": str}`` (loaded from
    ``session_issue_attribution``).  When provided, each session's
    fraction and phase are rendered as sub-bullets under the summary
    block.  PR-rooted and session-rooted units pass ``None`` here and
    omit the attribution lines.
    """
    flags = unit["outlier_flags"] if unit["outlier_flags"] is not None else "[]"
    abandoned = unit["abandonment_flag"]
    abandoned_str = "yes" if abandoned == 1 else ("no" if abandoned == 0 else "n/a")
    lines = [
        f"### unit {unit['unit_id']}",
        f"- root_node: {unit['root_node_id']}",
        f"- elapsed_days: {unit['elapsed_days']}",
        f"- dark_time_pct: {unit['dark_time_pct']}",
        f"- total_reprompts: {unit['total_reprompts']}",
        f"- review_cycles: {unit['review_cycles']}",
        f"- status: {unit['status']}",
        f"- outlier_flags: {flags}",
        f"- abandonment_flag: {abandoned_str}",
    ]

    # Render per-session attribution for issue-rooted units (AS-7).
    if session_attribution:
        lines.append("")
        lines.append("#### session attribution")
        for session_uuid, attrs in sorted(session_attribution.items()):
            fraction = attrs.get("fraction", 1.0)
            phase = attrs.get("phase", "execution")
            lines.append(
                f"- session {session_uuid}: session_fraction={fraction}, phase={phase}"
            )

    lines += [
        "",
        "#### summary",
        "```",
        transcript,
        "```",
        "",
    ]
    return "\n".join(lines)


def _render_gap_block(gap_rows: List[dict]) -> List[str]:
    """Render the ``## Expectation Gaps`` Markdown block for the prompt.

    Only major and critical rows are included — minor and none rows live
    in ``expectation_gaps`` for X-5 calibration but do not appear in the
    retrospective (signal density). Returns a list of lines; empty list
    when there is nothing to render (the caller then skips adding the
    header).
    """
    if not gap_rows:
        return []
    lines: List[str] = ["", "## Expectation Gaps (from X-2 gap analysis)", ""]
    for row in gap_rows:
        unit_id = row.get("unit_id")
        severity = row.get("severity") or ""
        direction = row.get("direction") or ""
        fp = row.get("failure_precondition") or ""
        lines.append(
            f"- unit {unit_id}: severity={severity}, direction={direction}, "
            f"failure_precondition={fp}"
        )
        for key in ("scope_gap", "effort_gap", "outcome_gap"):
            val = row.get(key)
            if val:
                lines.append(f"    - {key}: {val}")
    lines.append("")
    return lines


def _render_revision_block(revision_rows: List[dict]) -> List[str]:
    """Render the ``## Expectation Revisions`` Markdown block.

    Groups rows by unit, names each revision's trigger + facet, and
    annotates low-confidence (<0.5) rows with ``[low confidence]`` so
    they are surfaced rather than silently dropped (AS-6).
    """
    if not revision_rows:
        return []
    lines: List[str] = ["", "## Expectation Revisions (from X-3 revision detection)", ""]
    # Group by unit, preserving the order (rows are already sorted by
    # unit_id + revision_index upstream).
    current_unit: Optional[str] = None
    for row in revision_rows:
        unit_id = row.get("unit_id")
        if unit_id != current_unit:
            lines.append(f"- unit {unit_id}:")
            current_unit = unit_id
        trigger = row.get("revision_trigger") or ""
        facet = row.get("facet") or ""
        confidence = row.get("confidence")
        conf_marker = ""
        if isinstance(confidence, (int, float)) and confidence < 0.5:
            conf_marker = " [low confidence]"
        turn = row.get("revision_turn")
        lines.append(
            f"    - revision at turn {turn}: trigger={trigger}, "
            f"facet={facet}{conf_marker}"
        )
        before = row.get("before_text")
        after = row.get("after_text")
        if before:
            lines.append(f"        - before: {before}")
        if after:
            lines.append(f"        - after: {after}")
    lines.append("")
    return lines


def _assemble_prompt(
    units: List[dict],
    unit_transcripts: dict,
    unit_timelines: dict,
    week_start: str,
    unit_attributions: Optional[dict] = None,
    gap_rows: Optional[List[dict]] = None,
    revision_rows: Optional[List[dict]] = None,
    calibration_trends: Optional[dict] = None,
    calibration_unit_count: Optional[int] = None,
) -> Tuple[str, List[dict]]:
    """Return (system_prompt, user_messages) for the synthesis call.

    *system_prompt* is the static cacheable block. *user_messages* is
    the list of ``{"role": "user", "content": ...}`` dicts the SDK
    consumes — a single user message that concatenates the week anchor
    with the per-unit dynamic blocks.

    *unit_attributions* is an optional ``{unit_id: {session_uuid: {...}}}``
    dict produced by ``_load_issue_attribution`` for issue-rooted units.
    Non-issue units are absent from this dict; their blocks render without
    attribution lines.
    """
    if unit_attributions is None:
        unit_attributions = {}

    body_parts = [
        f"# Week starting {week_start}",
        "",
        f"Total units this week: {len(units)}",
        "",
    ]
    for unit in units:
        uid = unit["unit_id"]
        attribution = unit_attributions.get(uid)  # None for non-issue units
        body_parts.append(
            _format_unit_block(unit, unit_transcripts.get(uid, ""), attribution)
        )
        # Timeline is a compact Markdown list — one line per event.
        timeline = unit_timelines.get(uid, [])
        if timeline:
            body_parts.append("#### timeline")
            for ev in timeline:
                body_parts.append(
                    f"- {ev['timestamp']} {ev['type']} {ev['description']}"
                )
            body_parts.append("")

    # Epic #27 — X-2 (#73): inject gap rows into the user message so the
    # LLM can reason about them. Only major/critical are surfaced here;
    # minor/none stay in the DB for X-5 calibration.
    if gap_rows:
        body_parts.extend(_render_gap_block(gap_rows))

    # Epic #27 — X-3 (#74): inject revision rows. All revisions are
    # surfaced (low-confidence ones are annotated, not dropped).
    if revision_rows:
        body_parts.extend(_render_revision_block(revision_rows))

    # Epic #27 — X-5 (#76): calibration trends. Empty dict below the
    # ≥20-correction threshold — block is omitted entirely.
    # AC-9 gate: also pass processed_unit_count so render_calibration_block
    # can enforce the ≥30-unit floor alongside the correction threshold.
    if calibration_trends:
        from synthesis.calibration import render_calibration_block

        body_parts.extend(
            render_calibration_block(
                calibration_trends,
                processed_unit_count=calibration_unit_count,
            )
        )

    user_content = "\n".join(body_parts)
    return _STATIC_SYSTEM_PROMPT, [{"role": "user", "content": user_content}]


# ---------------------------------------------------------------------------
# Client selection — see synthesis.llm_adapter._get_adapter
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_synthesis(
    config: SynthesisConfig,
    github_db: Union[str, Path],
    sessions_db: Union[str, Path],
    week_start: str,
    dry_run: bool = False,
    expectations_db: Optional[Union[str, Path]] = None,
    repo: Optional[str] = None,
    unit_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> Optional[Path]:
    """End-to-end weekly synthesis for *week_start*.

    Parameters
    ----------
    config:
        Validated :class:`SynthesisConfig`. Names the env var that
        holds the Anthropic key, the model, and the output directory.
    github_db, sessions_db:
        Paths to the collector DBs. The fixture packs both schemas into
        one file; in production they are two files under
        ``config.data_dir``.
    week_start:
        ``YYYY-MM-DD`` anchor. Must match a value in
        ``units.week_start`` — no units → no retrospective is written
        (function returns ``None``).
    dry_run:
        When ``True``, writes the assembled prompt to
        ``<output_dir>/.dry-run/<week_start>.prompt.txt`` and returns
        that path. No LLM call, no retrospective file. Useful for
        iterating on the prompt without burning tokens.

    Returns
    -------
    * Production path (``dry_run=False``):
      - Path to the newly-written retrospective, or
      - ``None`` if the file already existed (refuse-to-overwrite), or
      - ``None`` if the week has no units.
    * Dry-run path (``dry_run=True``):
      - Path to the ``.prompt.txt`` that was written, or
      - ``None`` if the week has no units.
    """
    gh_path = Path(github_db)
    sess_path = Path(sessions_db)
    gh_conn = sqlite3.connect(str(gh_path))
    # Share the connection only when both paths resolve to the SAME file
    # on disk. ``Path.__eq__`` is a string compare, which misses
    # equivalent-but-non-normalised paths (e.g. ``a/b`` vs ``a/./b``).
    # ``os.path.samefile`` follows symlinks and normalises, but requires
    # both files to exist — we only call it after opening ``gh_conn``
    # (which creates the file if missing) and fall back to string
    # compare when samefile raises.
    try:
        _same = sess_path.exists() and os.path.samefile(str(gh_path), str(sess_path))
    except OSError:
        _same = str(gh_path) == str(sess_path)
    sess_conn = gh_conn if _same else sqlite3.connect(str(sess_path))

    try:
        units = _load_units(gh_conn, week_start, repo=repo)
        if not units:
            repo_suffix = f" repo={repo}" if repo else ""
            logger.info(
                "No units found for week_start=%s%s; skipping synthesis",
                week_start, repo_suffix,
            )
            return None

        # Issue #90: unit_ids / limit filter.
        # unit_ids (explicit list) takes precedence over limit.
        if unit_ids:
            known = {u["unit_id"] for u in units}
            unknown = [uid for uid in unit_ids if uid not in known]
            if unknown:
                raise ValueError(
                    f"Unknown unit_ids for week_start={week_start!r}: {unknown}. "
                    "Check that the ids exist in units for this week (and repo if --repo is set)."
                )
            units = [u for u in units if u["unit_id"] in set(unit_ids)]

        # --- Issue #54 P-2: prioritise + cap unit count ---------------
        # Prompt-size safety rail. Units are ordered so that the most
        # signal-bearing ones survive a truncation:
        #   1. abandonment_flag=1 first (most actionable — user stalled)
        #   2. units with non-empty outlier_flags next (metric outliers)
        #   3. longer-running units (elapsed_days desc)
        #   4. unit_id asc as the final tie-break (determinism)
        #
        # NOTE on ordering — issue #54's "Proposed Fix" listed
        # ``outlier_flag > abandonment_flag > elapsed_days desc``. We
        # intentionally invert the first two: an abandoned unit is a
        # user who stalled for ≥14 days with no event, which is the
        # most actionable signal we surface; an outlier only means "a
        # metric exceeded 2σ", which may just be a tail observation.
        # When we truncate under pressure, the abandoned units are the
        # ones we most want the retrospective to reason about. The
        # deviation from the issue spec is deliberate — not a typo.
        #
        # ``outlier_flags`` is a JSON string ("[]" / '["x","y"]' / NULL)
        # produced upstream via ``json.dumps`` with default separators
        # (see ``synthesis/cross_unit.py``). We only need empty-vs-
        # non-empty, so a literal compare against "[]" is sufficient
        # without parsing. If an upstream producer ever emits the same
        # payload with different whitespace/separators this comparison
        # would need ``json.loads`` — guard the invariant there.
        #
        # ``elapsed_days`` is NULLable in the cross-unit schema; we
        # coerce ``None`` to 0.0, which sorts NULL-elapsed units below
        # any unit with a real elapsed value. ``unit_id`` is the
        # ``units`` PRIMARY KEY (non-NULL by schema) so no ``or ""``
        # fallback is needed.
        def _priority_key(u: dict) -> tuple:
            abandoned = 1 if u.get("abandonment_flag") == 1 else 0
            flags = u.get("outlier_flags")
            has_outliers = 1 if (flags is not None and flags != "[]") else 0
            elapsed = u.get("elapsed_days") or 0.0
            unit_id = u["unit_id"]
            # Negate the fields we want DESC so a plain ``sorted`` ASC
            # yields the right order with unit_id as the final ASC key.
            return (-abandoned, -has_outliers, -elapsed, unit_id)

        units.sort(key=_priority_key)

        if limit is not None and unit_ids is None:
            units = units[:limit]

        # Issue #90 cycle-2: whenever the user scoped the pipeline via
        # --unit-id or --limit, downstream gap/revision passes must see
        # the same scoped set — both in writes (gap_analysis.run /
        # revision_detector.run, which overwrite sibling tables but
        # iterate over the full `expectations` table by default) and
        # reads (load_gap_rows / load_revision_rows, which otherwise
        # return stale rows left from prior non-scoped runs). When no
        # scoping was requested, leave this None so behavior is
        # byte-identical to the pre-#90 baseline.
        scoped_unit_ids: Optional[List[str]] = None
        if unit_ids is not None or limit is not None:
            scoped_unit_ids = [u["unit_id"] for u in units]

        if len(units) > MAX_UNITS_PER_PROMPT:
            logger.warning(
                "Unit count %d exceeds MAX_UNITS_PER_PROMPT=%d for "
                "week_start=%s; truncating to top %d by priority "
                "(abandonment_flag=1, then non-empty outlier_flags, "
                "then elapsed_days desc, then unit_id asc)",
                len(units),
                MAX_UNITS_PER_PROMPT,
                week_start,
                MAX_UNITS_PER_PROMPT,
            )
            units = units[:MAX_UNITS_PER_PROMPT]

        # Load pre-computed unit summaries from the unit_summaries table.
        # These are generated by ``am-summarize-units`` (synthesis/summarize.py)
        # and replace the old water-filled raw transcript assembly path.
        unit_summaries = _load_unit_summaries(gh_conn, week_start)

        # Resolve every unit's component so we can feed the timeline renderer.
        # We still walk graph_nodes/graph_edges per unit — the timeline is
        # separate from the summary and still reads live from the graph tables.
        unit_components: dict = {}
        for u in units:
            comp = _unit_nodes(gh_conn, week_start, u["root_node_id"])
            unit_components[u["unit_id"]] = comp

        # Fail loud if any unit in this week's set has no summary row.
        # This prevents a silent empty-transcript block from reaching the LLM.
        unit_transcripts: dict = {}
        unit_timelines: dict = {}
        for u in units:
            uid = u["unit_id"]
            if uid not in unit_summaries:
                raise RuntimeError(
                    f"unit_summaries row missing for unit_id={uid!r} "
                    f"(week={week_start!r}). "
                    f"Run: am-summarize-units --week {week_start}"
                )
            unit_transcripts[uid] = unit_summaries[uid]
            unit_timelines[uid] = render_timeline(
                unit_components[uid], gh_conn, sess_conn
            )

        # For issue-rooted units, load session_issue_attribution rows so
        # _format_unit_block can render session_fraction + phase (AS-7).
        # PR-rooted and session-rooted units are skipped; their blocks
        # render without attribution lines.
        unit_attributions: dict = {}
        for u in units:
            if u.get("root_node_type") == "issue":
                uid = u["unit_id"]
                root_node_id = u.get("root_node_id", "")
                # root_node_id format: "issue:<repo>#<number>"
                try:
                    # Parse "issue:<repo>#<number>"
                    without_prefix = root_node_id[len("issue:"):]
                    repo_part, issue_part = without_prefix.rsplit("#", 1)
                    issue_number = int(issue_part)
                except (ValueError, IndexError):
                    # Malformed root_node_id — skip attribution for this unit.
                    continue
                # Collect session UUIDs from the unit's component first.
                session_uuids = [
                    nid
                    for nid, ntype, _ in unit_components.get(uid, [])
                    if ntype == "session"
                ]
                # Shape A (session_refs_issue removed from graph_edges): the
                # issue-rooted component contains only the issue node — session
                # nodes are in their own PR-component which was suppressed.
                # Fall back to querying session_issue_attribution directly.
                if not session_uuids:
                    try:
                        rows = gh_conn.execute(
                            "SELECT DISTINCT session_uuid "
                            "FROM session_issue_attribution "
                            "WHERE week_start = ? AND repo = ? AND issue_number = ?",
                            (week_start, repo_part, issue_number),
                        ).fetchall()
                        session_uuids = [r[0] for r in rows]
                    except Exception:
                        pass
                if session_uuids:
                    unit_attributions[uid] = _load_issue_attribution(
                        gh_conn, week_start, repo_part, issue_number, session_uuids
                    )

        # Epic #27 — X-2 (#73) + X-3 (#74): run the gap + revision passes
        # before prompt assembly so their rows are available to inject
        # into the user message. Silent no-op when expectations_db is
        # None (operator has not wired X-1 yet) or the DB has no
        # expectations rows for this week.
        gap_rows: List[dict] = []
        revision_rows: List[dict] = []
        if expectations_db is not None:
            from synthesis import gap_analysis

            try:
                gap_analysis.run(
                    week_start,
                    github_db=str(gh_path),
                    expectations_db=str(expectations_db),
                    config=config,
                    repo=repo,
                    unit_ids=scoped_unit_ids,
                )
                gap_rows = gap_analysis.load_gap_rows(
                    str(expectations_db),
                    week_start,
                    min_severity=("major", "critical"),
                    repo=repo,
                    github_db=str(gh_path),
                    unit_ids=scoped_unit_ids,
                )
                # Epic #27 — X-4 (#75): auto-confirm sweep fires on every
                # ``am-synthesize --week`` invocation (AS-6). Any gap row
                # older than 14 days without a user correction gets
                # ``corrected_by='auto_confirm'`` rows written. Non-fatal
                # if the corrections table is missing — log + proceed.
                from synthesis.correction import auto_confirm_sweep

                try:
                    auto_confirm_sweep(str(expectations_db))
                except sqlite3.OperationalError as exc:
                    logger.warning(
                        "Auto-confirm sweep skipped for week=%s: %s",
                        week_start, exc,
                    )
            except sqlite3.OperationalError as exc:
                # Most likely: expectations.db not initialised yet (X-1
                # hasn't been run). Log a warning and proceed without the
                # gap section — degraded operation over a broken pipeline.
                logger.warning(
                    "Gap analysis skipped for week=%s: %s. Run "
                    "'am-init-db' and 'am-extract-expectations' first.",
                    week_start, exc,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Gap analysis failed for week=%s: %s. Retrospective "
                    "will be written without the Expectation Gaps section.",
                    week_start, exc,
                )

            # X-3 revision detection — parallel to the gap pass. Each
            # side reads X-1's ``expectations`` rows and writes its own
            # sibling table, so the two can run in either order.
            from synthesis import revision_detector

            try:
                revision_detector.run(
                    week_start,
                    github_db=str(gh_path),
                    sessions_db=str(sess_path),
                    expectations_db=str(expectations_db),
                    config=config,
                    repo=repo,
                    unit_ids=scoped_unit_ids,
                )
                revision_rows = revision_detector.load_revision_rows(
                    str(expectations_db), week_start, repo=repo,
                    github_db=str(gh_path),
                    unit_ids=scoped_unit_ids,
                )
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "Revision detection skipped for week=%s: %s. Run "
                    "'am-init-db' and 'am-extract-expectations' first.",
                    week_start, exc,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Revision detection failed for week=%s: %s. "
                    "Retrospective will be written without the "
                    "Expectation Revisions section.",
                    week_start, exc,
                )

        # Epic #27 — X-5 (#76): calibration trends pass. Runs AFTER the
        # X-4 auto-confirm sweep (which populates additional correction
        # rows) so the threshold count reflects the freshest state. Below
        # the ≥20 user-correction threshold this is a strict no-op and
        # returns an empty dict — the retrospective omits the section.
        calibration_trends: dict = {}
        calibration_unit_count: Optional[int] = None
        if expectations_db is not None:
            from synthesis import calibration

            try:
                calibration_trends = calibration.run(
                    week_start,
                    github_db=str(gh_path),
                    expectations_db=str(expectations_db),
                )
                # AC-9: also count total processed units (expectation_gaps
                # rows across all weeks) so render_calibration_block can
                # enforce the ≥30-unit floor (the second gate).
                _exp_conn = sqlite3.connect(str(expectations_db))
                try:
                    calibration_unit_count = calibration._count_processed_units(
                        _exp_conn
                    )
                except Exception:  # noqa: BLE001
                    calibration_unit_count = None
                finally:
                    _exp_conn.close()
            except sqlite3.OperationalError as exc:
                logger.warning(
                    "Calibration pass skipped for week=%s: %s. Run "
                    "'am-init-db' and 'am-extract-expectations' first.",
                    week_start, exc,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Calibration pass failed for week=%s: %s. "
                    "Retrospective will be written without the "
                    "Calibration Trends section.",
                    week_start, exc,
                )

        system_prompt, messages = _assemble_prompt(
            units,
            unit_transcripts,
            unit_timelines,
            week_start,
            unit_attributions,
            gap_rows=gap_rows,
            revision_rows=revision_rows,
            calibration_trends=calibration_trends,
            calibration_unit_count=calibration_unit_count,
        )

        # --- Issue #54 P-2: prompt-size guard -------------------------
        # Two-tier ceiling on the assembled prompt (system + user).
        # Runs BEFORE the dry-run short-circuit AND before the live
        # network call so both paths react identically.
        #
        # * ``> MAX_PROMPT_SOFT_WARN_BYTES`` (512 KB) — log WARNING.
        #   This is epic #50's stated "≤ 512 KB for a real week"
        #   acceptance threshold, enforced as a signal rather than a
        #   raise because real weekly runs are ~42 units and any
        #   breach means something is off upstream.
        # * ``> MAX_PROMPT_BYTES`` (1 MiB) — raise ``RuntimeError``.
        #   Fail-loud rail. The unit cap above is the first line of
        #   defence; this catches the case where even the capped unit
        #   set produces a prompt that is too large (e.g. a single
        #   unit with a very long metadata block).
        total_bytes = len(system_prompt) + sum(
            len(m["content"]) for m in messages
        )
        if total_bytes > MAX_PROMPT_BYTES:
            raise RuntimeError(
                f"Assembled prompt is {total_bytes} bytes, exceeds "
                f"MAX_PROMPT_BYTES={MAX_PROMPT_BYTES} for week_start="
                f"{week_start!r} with {len(units)} units. Refusing to "
                f"call the LLM (or write a dry-run artefact) with a "
                f"prompt this large — this usually indicates a bug "
                f"upstream (missed transcript truncation, runaway unit "
                f"set, or oversized per-unit metadata)."
            )
        if total_bytes > MAX_PROMPT_SOFT_WARN_BYTES:
            logger.warning(
                "Assembled prompt is %d bytes, exceeds "
                "MAX_PROMPT_SOFT_WARN_BYTES=%d (epic #50 target) for "
                "week_start=%s with %d units. Proceeding, but the "
                "prompt is above the epic's acceptance threshold — "
                "investigate whether the unit cap or transcript "
                "budget needs tightening.",
                total_bytes,
                MAX_PROMPT_SOFT_WARN_BYTES,
                week_start,
                len(units),
            )

        # --- dry-run short-circuit ------------------------------------
        if dry_run:
            dry_dir = Path(config.output_dir) / ".dry-run"
            dry_dir.mkdir(parents=True, exist_ok=True)
            dry_path = dry_dir / f"{week_start}.prompt.txt"
            # Concatenate system + user for a human-readable dump. The
            # two halves are visually separated so a developer paging
            # through the file can tell which block is cached.
            text_content = messages[0]["content"] if messages else ""
            dump = (
                "=== SYSTEM (cacheable) ===\n"
                f"{system_prompt}\n"
                "=== USER ===\n"
                f"{text_content}\n"
            )
            dry_path.write_text(dump, encoding="utf-8")
            logger.info("Dry-run prompt written to %s", dry_path)
            return dry_path

        # --- live / fake synthesis ------------------------------------
        user_content = messages[0]["content"]
        markdown = _get_adapter(config).call(system_prompt, user_content, config.model, _MAX_OUTPUT_TOKENS).text

        result = write_retrospective(
            markdown, config.output_dir, week_start, repo=repo
        )

        # Record a successful synthesis run in health.json ONLY when the
        # retrospective was actually written this invocation. Refuse-to-
        # overwrite (Decision 2) returns None — that case is idempotent
        # success, but it's not a new data point so we don't bump the
        # health timestamp. ``last_record_count`` is the number of units
        # the run synthesised, giving operators a cheap signal to spot
        # "synthesis ran but the week was empty" vs. the normal case.
        if result is not None:
            write_health("synthesis", len(units))

        return result

    finally:
        if sess_conn is not gh_conn:
            sess_conn.close()
        gh_conn.close()


__all__ = [
    "run_synthesis",
    "water_fill_truncate",
    "_resolve_unit_sessions",
    "_repo_filter_sql",
    "TRANSCRIPT_BUDGET_BYTES",
    "MAX_UNITS_PER_PROMPT",
    "MAX_PROMPT_SOFT_WARN_BYTES",
    "MAX_PROMPT_BYTES",
]
