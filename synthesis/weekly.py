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
* ``_get_client`` — picks between live SDK and
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
from synthesis.fake_client import FakeAnthropicClient
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
#   ``raise RuntimeError`` rather than ship — a bloated prompt that
#   size points to a bug upstream (missed truncation, runaway unit set,
#   oversized per-unit metadata) and silently truncating it would hide
#   the signal. 1 MiB is ~2x the transcript budget (512 KB) plus
#   headroom for the static template + per-unit metadata blocks.
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


def _load_units(
    github_conn: sqlite3.Connection,
    week_start: str,
) -> List[dict]:
    """Return one dict per ``units`` row for *week_start*.

    Includes ``outlier_flags`` (JSON string or NULL) and
    ``abandonment_flag`` (0/1 or NULL) from the cross-unit pass.
    """
    rows = github_conn.execute(
        "SELECT unit_id, root_node_type, root_node_id, "
        "       elapsed_days, dark_time_pct, total_reprompts, "
        "       review_cycles, status, outlier_flags, abandonment_flag "
        "FROM units WHERE week_start = ? "
        "ORDER BY unit_id",
        (week_start,),
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
6. `## Clarifying Questions`      — at most TWO total, numbered `1.`
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


def _format_unit_block(unit: dict, transcript: str) -> str:
    """Render one unit's dynamic block for the prompt.

    Includes the metrics, flags, and (potentially truncated) transcript.
    Keeps the Markdown readable by an LLM — table-less, heading-heavy.

    ``outlier_flags`` and ``abandonment_flag`` are NULLable in the
    ``units`` schema (see cross_unit.py migration). We expect both keys
    to be present — ``_load_units`` always populates them — so the
    explicit ``is None`` check handles the NULL case only, not a
    missing key (which would be a programming error elsewhere).
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
        "",
        "#### transcript",
        "```",
        transcript,
        "```",
        "",
    ]
    return "\n".join(lines)


def _assemble_prompt(
    units: List[dict],
    unit_transcripts: dict,
    unit_timelines: dict,
    week_start: str,
) -> Tuple[str, List[dict]]:
    """Return (system_prompt, user_messages) for the synthesis call.

    *system_prompt* is the static cacheable block. *user_messages* is
    the list of ``{"role": "user", "content": ...}`` dicts the SDK
    consumes — a single user message that concatenates the week anchor
    with the per-unit dynamic blocks.
    """
    body_parts = [
        f"# Week starting {week_start}",
        "",
        f"Total units this week: {len(units)}",
        "",
    ]
    for unit in units:
        body_parts.append(_format_unit_block(unit, unit_transcripts.get(unit["unit_id"], "")))
        # Timeline is a compact Markdown list — one line per event.
        timeline = unit_timelines.get(unit["unit_id"], [])
        if timeline:
            body_parts.append("#### timeline")
            for ev in timeline:
                body_parts.append(
                    f"- {ev['timestamp']} {ev['type']} {ev['description']}"
                )
            body_parts.append("")

    user_content = "\n".join(body_parts)
    return _STATIC_SYSTEM_PROMPT, [{"role": "user", "content": user_content}]


# ---------------------------------------------------------------------------
# Client selection
# ---------------------------------------------------------------------------


def _get_client(config: SynthesisConfig) -> Tuple[object, bool]:
    """Return ``(client, is_live)`` for the synthesis call.

    * ``AMIS_SYNTHESIS_LIVE`` unset / falsy → ``(FakeAnthropicClient(), False)``.
    * ``AMIS_SYNTHESIS_LIVE=1``              → ``(anthropic.Anthropic(...), True)``.
      Import is lazy so an unset ``anthropic`` install does not break
      offline runs. The ``ANTHROPIC_API_KEY`` env var (or whichever var
      ``config.anthropic_api_key_env`` names) must be set — the SDK
      picks it up via its usual env-var dance.

    Returning the ``is_live`` flag alongside the client lets
    :func:`_call_llm` pick the system-prompt shape (plain string vs.
    list-of-blocks with ``cache_control``) without re-reading the env
    var. The flag that drove client selection is also the flag that
    drives the cache shape — they cannot drift.
    """
    if not os.environ.get("AMIS_SYNTHESIS_LIVE"):
        return FakeAnthropicClient(), False

    # Live path — lazy import so the anthropic dep is not required at
    # import time, only when actually called with AMIS_SYNTHESIS_LIVE=1.
    import anthropic  # noqa: WPS433 — deliberate lazy import

    api_key = os.environ.get(config.anthropic_api_key_env)
    if not api_key:
        raise RuntimeError(
            f"AMIS_SYNTHESIS_LIVE is set but {config.anthropic_api_key_env} "
            f"is empty — cannot call the Anthropic API"
        )
    return anthropic.Anthropic(api_key=api_key), True


def _call_llm(
    client,
    config: SynthesisConfig,
    system_prompt: str,
    messages: list,
    is_live: bool,
) -> str:
    """Dispatch the synthesis call and return the Markdown text.

    When *is_live* is True, the system prompt is wrapped in a
    list-of-blocks shape with ``cache_control`` so the Anthropic API
    treats it as an ephemeral cache entry. The fake client accepts the
    same shape (ignores the caching hint) so the call site needs no
    branching — but we still use the plain form offline so the offline
    path never silently depends on the SDK's block parser.

    *is_live* comes from :func:`_get_client` so the flag that drove
    client selection is also the flag that drives the cache shape;
    they cannot drift via an env var being toggled mid-run.
    """
    if is_live:
        # List-of-blocks form enables prompt caching on the static
        # context. See https://docs.anthropic.com/en/docs/build-with-claude/prompt-caching
        system_blocks = [
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ]
    else:
        # Fake client accepts either a plain string or the blocks form.
        # We use the plain form offline so the offline path never
        # silently depends on the SDK's block parser.
        system_blocks = system_prompt

    resp = client.messages.create(
        model=config.model,
        max_tokens=_MAX_OUTPUT_TOKENS,
        system=system_blocks,
        messages=messages,
    )
    # Both the real SDK's Message and our FakeMessage expose
    # ``content[0].text`` for a single-block text response, which is
    # what a non-tool-use synthesis call returns.
    return resp.content[0].text


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_synthesis(
    config: SynthesisConfig,
    github_db: Union[str, Path],
    sessions_db: Union[str, Path],
    week_start: str,
    dry_run: bool = False,
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
        units = _load_units(gh_conn, week_start)
        if not units:
            logger.info(
                "No units found for week_start=%s; skipping synthesis", week_start
            )
            return None

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

        # Resolve every unit's component so we can pull the session
        # UUIDs (for transcripts) and feed the timeline renderer.
        unit_components: dict = {}
        all_session_uuids: List[str] = []
        per_unit_uuids: dict = {}
        for u in units:
            comp = _unit_nodes(gh_conn, week_start, u["root_node_id"])
            unit_components[u["unit_id"]] = comp
            uuids = [nr for nid, nt, nr in comp if nt == "session" and nr]
            per_unit_uuids[u["unit_id"]] = uuids
            all_session_uuids.extend(uuids)

        # Water-fill the transcripts across ALL sessions in ALL units
        # so the 512 KB budget is global, not per-unit. ADR Decision 4
        # phrases it as "cumulative budget".
        raw = _load_session_transcripts(sess_conn, all_session_uuids)
        # raw is aligned with all_session_uuids by construction.
        truncated = water_fill_truncate(
            [content for _uid, content in raw],
            TRANSCRIPT_BUDGET_BYTES,
        )
        by_uuid = {uid: tc for (uid, _), tc in zip(raw, truncated)}

        # Per-unit transcript = concatenation of the unit's (truncated)
        # sessions. Blank units yield an empty transcript block — the
        # prompt still renders them so the LLM can reason about
        # "why is this unit empty".
        unit_transcripts: dict = {}
        unit_timelines: dict = {}
        for u in units:
            parts = [by_uuid.get(uid, "") for uid in per_unit_uuids[u["unit_id"]]]
            unit_transcripts[u["unit_id"]] = "\n\n".join(p for p in parts if p)
            unit_timelines[u["unit_id"]] = render_timeline(
                unit_components[u["unit_id"]], gh_conn, sess_conn
            )

        system_prompt, messages = _assemble_prompt(
            units, unit_transcripts, unit_timelines, week_start
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
        client, is_live = _get_client(config)
        markdown = _call_llm(client, config, system_prompt, messages, is_live)

        result = write_retrospective(markdown, config.output_dir, week_start)

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
    "TRANSCRIPT_BUDGET_BYTES",
    "MAX_UNITS_PER_PROMPT",
    "MAX_PROMPT_SOFT_WARN_BYTES",
    "MAX_PROMPT_BYTES",
]
