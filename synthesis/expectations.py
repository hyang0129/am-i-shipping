"""Epic #27 — X-1 (#72): per-unit expectation extraction.

Reads the ``units`` and ``graph_*`` tables populated by Phase 2, plus session
transcripts from ``sessions.db``, and for each unit invokes Sonnet (or the
offline :class:`FakeAnthropicClient` in test mode) to identify the
**commitment point** — the user turn where an implicit or explicit plan is
accepted — and classify four expectation facets at that point: ``scope``
(what files/surfaces), ``effort`` (how many sessions / reprompts / days the
user implicitly expected), ``outcome`` (what "done" means), and
``confidence`` (0-1 self-reported by the classifier).

Output rows land in a new ``expectations.db`` created by
:func:`am_i_shipping.db.init_expectations_db`.

Behavioral invariants (from the refined spec):

* **Every unit in the week gets at least one row.** Either a populated
  expectation row, or exactly one row with a non-NULL ``skip_reason``
  naming the cause. No unit is ever silently absent from the table.
* **Hybrid commitment-point detection.** A pure structural detector
  (last user text turn before the first tool-use turn) produces a
  candidate; that candidate plus ±2 surrounding text turns are sent to
  Sonnet, which either confirms or reassigns it. The final
  ``commitment_point`` and ``confidence`` are recorded as-returned — low
  confidence rows are NOT silently suppressed.
* **Idempotency.** Re-running without ``--rebuild`` issues zero new LLM
  calls for units already present in ``expectations``.
* **Diagnostic logging.** The per-unit ``input_bytes`` value is logged at
  INFO level (surfaces the system-feared failure mode: semantically empty
  input producing structurally-valid-but-meaningless expectations), and
  the run-level structural-vs-LLM commitment-point agreement rate is
  emitted when the run completes.

Architecture notes
------------------
* Reuses :func:`synthesis.weekly._unit_nodes` and
  :func:`synthesis.weekly._load_session_transcripts` unchanged — these are
  the graph-walk + session-lookup helpers the rest of the synthesis
  pipeline shares.
* Reuses :func:`synthesis.llm_adapter._get_adapter` for offline/live
  client selection. ``AMIS_SYNTHESIS_LIVE`` unset (the default in tests)
  routes to :class:`FakeAnthropicClient`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

from am_i_shipping.config_loader import SynthesisConfig, load_config
from am_i_shipping.db import init_expectations_db, init_github_db
from synthesis.llm_adapter import _get_adapter
from synthesis.weekly import (
    _load_session_transcripts,
    _load_units,
    _repo_filter_sql,
    _resolve_unit_sessions,
    _unit_nodes,
)


logger = logging.getLogger(__name__)


# Upper bound on the classifier's JSON output. A populated facet block is
# a handful of short strings; 1024 tokens is generous headroom.
_MAX_OUTPUT_TOKENS = 1024


_EXPECTATIONS_SYSTEM_PROMPT = """You are extracting the user's expectations at the \
commitment point of a software development unit for a retrospective \
calibration system.

A "unit" is one complete development cycle (issue -> PR -> merge or \
abandon). The "commitment point" is the user turn where an implicit or \
explicit plan is accepted by the user — the moment after which execution \
is expected to proceed without further design-phase disambiguation.

You will be given (1) a structurally-detected candidate commitment turn \
and (2) the ±2 surrounding user text turns from the transcript. Decide \
whether the candidate is correct or reassign it, then classify four \
facets of what the user expected at that moment:

- expected_scope: which files / surfaces / modules the user expected to \
change. Prefer concrete names if mentioned; otherwise a phrase.
- expected_effort: how much work the user implicitly expected. Free-text \
matching the raw user language (e.g. "one session, ~2 hours", "a few \
reprompts", "one afternoon").
- expected_outcome: what "done" means for this unit, as the user framed \
it at the commitment point.
- confidence: your self-reported certainty on the overall classification, \
as a float in [0.0, 1.0].

Return a single JSON object with exactly these keys:
  commitment_point, expected_scope, expected_effort, expected_outcome, confidence

commitment_point should be a short string identifying the turn (e.g.
"turn 5: 'go ahead'" or "candidate confirmed"). If you cannot identify a \
commitment point from the supplied context, return null for \
commitment_point and the other facet strings, and set confidence to 0.0.

Do NOT add extra keys. Do NOT wrap the JSON in markdown fences. Do NOT \
invent signals that are not present in the supplied turns."""


# ---------------------------------------------------------------------------
# Transcript parsing — structural commitment-point detection
# ---------------------------------------------------------------------------


def _extract_turns(raw_content_json: str) -> List[Dict[str, Any]]:
    """Parse a raw_content_json blob into a normalized turn list.

    Each returned dict has keys:
      role: "user" | "assistant" | "system" | other
      kind: "text" | "tool_use" | "tool_result" | "other"
      text: str (empty for non-text turns)
      index: int (position in the source transcript)

    The session_parser stores ``raw_content_json`` as a JSON-serialized
    list of message dicts. Formats seen in the wild:
      * Plain string content: ``{"role": "user", "content": "hi"}``
      * Structured content list:
        ``{"role": "assistant", "content": [{"type": "text", "text": "..."},
        {"type": "tool_use", ...}]}``
    We normalize both into one flat turn list.
    """
    if not raw_content_json:
        return []
    try:
        messages = json.loads(raw_content_json)
    except (ValueError, TypeError):
        return []
    if not isinstance(messages, list):
        return []

    turns: List[Dict[str, Any]] = []
    for idx, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        role = msg.get("role") or ""
        content = msg.get("content")

        if isinstance(content, str):
            turns.append(
                {"role": role, "kind": "text", "text": content, "index": idx}
            )
            continue

        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue
                btype = block.get("type") or ""
                if btype == "text":
                    turns.append(
                        {
                            "role": role,
                            "kind": "text",
                            "text": block.get("text") or "",
                            "index": idx,
                        }
                    )
                elif btype == "tool_use":
                    turns.append(
                        {
                            "role": role,
                            "kind": "tool_use",
                            "text": "",
                            "index": idx,
                        }
                    )
                elif btype == "tool_result":
                    turns.append(
                        {
                            "role": role,
                            "kind": "tool_result",
                            "text": "",
                            "index": idx,
                        }
                    )
                else:
                    turns.append(
                        {"role": role, "kind": "other", "text": "", "index": idx}
                    )
            continue

        # Unknown content shape — skip.
    return turns


def detect_structural_commitment_point(
    turns: Sequence[Dict[str, Any]],
) -> Optional[int]:
    """Return the index of the structurally-detected commitment turn, or None.

    Heuristic (per the refined spec):

    The commitment point is the **last user text turn before the first
    tool-use turn**. This captures the pattern "user plans, user accepts,
    assistant begins execution with tool calls".

    If there are no tool-use turns at all, falls back to the last user
    text turn in the transcript (if any).

    Pure function — no DB, no I/O. Unit-testable in isolation.
    """
    if not turns:
        return None

    # Find index of first tool-use turn.
    first_tool_use: Optional[int] = None
    for i, t in enumerate(turns):
        if t["kind"] == "tool_use":
            first_tool_use = i
            break

    # Walk user text turns; pick the last one before first_tool_use
    # (or the last one overall if no tool-use).
    upper = first_tool_use if first_tool_use is not None else len(turns)
    last_user_text: Optional[int] = None
    for i in range(upper):
        t = turns[i]
        if t["role"] == "user" and t["kind"] == "text" and t["text"].strip():
            last_user_text = i
    if last_user_text is not None:
        return last_user_text

    # Fallback: any user text turn anywhere in the transcript.
    for i, t in enumerate(turns):
        if t["role"] == "user" and t["kind"] == "text" and t["text"].strip():
            return i
    return None


def _surrounding_user_text(
    turns: Sequence[Dict[str, Any]],
    anchor_idx: int,
    window: int = 2,
) -> List[Tuple[int, str]]:
    """Return ``[(turn_idx, text), ...]`` for up to ``±window`` user text turns.

    Anchored on ``anchor_idx``. Only user text turns are returned; the
    anchor itself is included. Pure function.
    """
    if anchor_idx < 0 or anchor_idx >= len(turns):
        return []
    user_text_indices = [
        i
        for i, t in enumerate(turns)
        if t["role"] == "user" and t["kind"] == "text" and t["text"].strip()
    ]
    if anchor_idx not in user_text_indices:
        # Still surface anchor so the LLM can see it (even if non-text).
        return [(anchor_idx, turns[anchor_idx].get("text") or "")]
    pos = user_text_indices.index(anchor_idx)
    lo = max(0, pos - window)
    hi = min(len(user_text_indices), pos + window + 1)
    return [
        (user_text_indices[p], turns[user_text_indices[p]]["text"])
        for p in range(lo, hi)
    ]


# ---------------------------------------------------------------------------
# Unit input assembly
# ---------------------------------------------------------------------------


# Issue #86: refined-spec grounding. When a session attached to a unit invoked
# ``/refine-issue`` against a specific issue, the current ``issues.body`` is
# the refined spec (the skill edits the body in place after producing the
# refined structure). Surfacing that body as explicit context to the classifier
# fixes a latent bug where ``expected_scope`` was grounded in the session's
# raw user prompt prose ("fix the thing") rather than the post-refinement
# structured intent.
def _refined_issue_body_for_unit(
    gh_conn: sqlite3.Connection,
    session_uuids: List[str],
    root_node_id: str,
    root_node_type: str,
) -> Optional[Tuple[str, str, int]]:
    """Return ``(repo, issue_number, body)`` for the refined issue, or None.

    Checks the unit's sessions for a ``skill_invocations`` row with
    ``skill_name='refine-issue'``. When one is found with a resolved
    ``target_repo`` / ``target_ref``, loads the current ``issues.body`` (the
    post-refinement text). Falls back to the unit's root node when the root
    is an issue and the skill row has no resolved target — this handles the
    common case where ``/refine-issue`` was invoked implicitly on the issue
    the unit is rooted on.

    Returns ``None`` when no ``/refine-issue`` signal is present for the
    unit, or when the ``skill_invocations`` table does not exist yet (legacy
    DBs before Issue #86).
    """
    if not session_uuids:
        return None
    try:
        placeholders = ",".join("?" * len(session_uuids))
        row = gh_conn.execute(
            f"SELECT target_repo, target_ref FROM skill_invocations "
            f"WHERE skill_name = 'refine-issue' "
            f"  AND session_uuid IN ({placeholders}) "
            f"ORDER BY invoked_at ASC LIMIT 1",
            session_uuids,
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if row is None:
        return None

    target_repo, target_ref = row
    # Fallback: if the skill row did not resolve its target, use the unit's
    # root issue. graph nodes encode issues as ``root_node_id`` that refers
    # back to graph_nodes.node_ref in the form ``repo#number``.
    if (not target_repo or not target_ref) and root_node_type == "issue":
        node_row = gh_conn.execute(
            "SELECT node_ref FROM graph_nodes WHERE node_id = ? LIMIT 1",
            (root_node_id,),
        ).fetchone()
        if node_row and node_row[0] and "#" in node_row[0]:
            target_repo, _, target_ref = node_row[0].rpartition("#")

    if not target_repo or not target_ref:
        return None
    try:
        issue_number = int(target_ref)
    except (TypeError, ValueError):
        return None

    body_row = gh_conn.execute(
        "SELECT body FROM issues WHERE repo = ? AND issue_number = ?",
        (target_repo, issue_number),
    ).fetchone()
    if body_row is None or not body_row[0]:
        return None
    return target_repo, str(issue_number), body_row[0]


def _build_unit_input(
    gh_conn: sqlite3.Connection,
    sessions_conn: sqlite3.Connection,
    unit_id: str,
    week_start: str,
) -> Tuple[str, int, Optional[int], str]:
    """Assemble the classifier input for *unit_id*.

    Returns ``(input_text, input_bytes, structural_candidate_turn_idx,
    skip_reason)``. ``skip_reason`` is an empty string when extraction
    should proceed; otherwise it names the cause
    (``raw_content_json_empty`` / ``no_text_turns``).
    ``structural_candidate_turn_idx`` is None when no candidate could be
    found.
    """
    # Walk the unit's graph component to collect its session UUIDs.
    # For issue-rooted units, _resolve_unit_sessions also falls back to
    # session_issue_attribution when graph traversal yields no sessions.
    unit_row = gh_conn.execute(
        "SELECT root_node_id, root_node_type FROM units WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchone()
    root_node_id = unit_row[0] if unit_row else ""
    root_node_type = unit_row[1] if unit_row else ""
    session_uuids: List[str] = _resolve_unit_sessions(
        gh_conn, week_start, root_node_id or "", root_node_type or ""
    )

    transcripts = _load_session_transcripts(sessions_conn, session_uuids)
    # Concatenate turns from every transcript in the unit, preserving order.
    all_turns: List[Dict[str, Any]] = []
    have_any_content = False
    for _uid, content in transcripts:
        if not content:
            continue
        have_any_content = True
        turns = _extract_turns(content)
        all_turns.extend(turns)

    if not have_any_content:
        return "", 0, None, "raw_content_json_empty"

    # Structural candidate.
    candidate_idx = detect_structural_commitment_point(all_turns)
    if candidate_idx is None:
        return "", 0, None, "no_text_turns"

    parts: List[str] = []
    parts.append(f"## Unit: {unit_id}")
    parts.append(f"## Week: {week_start}")
    parts.append(
        f"## Structural commitment-point candidate: turn index {candidate_idx}"
    )
    parts.append("")

    # Issue #86: when /refine-issue was invoked for this unit's target issue,
    # prepend the refined issue body. The classifier should ground
    # expected_scope in the refined spec, not the raw user prompt prose.
    refined = _refined_issue_body_for_unit(
        gh_conn, session_uuids, root_node_id or "", root_node_type or ""
    )
    if refined is not None:
        ref_repo, ref_number, ref_body = refined
        parts.append(
            "### Refined issue body (post-/refine-issue — prefer this for expected_scope)"
        )
        parts.append(f"Source: {ref_repo}#{ref_number}")
        parts.append(ref_body)
        parts.append("")

    parts.append(
        "### All user text turns (full context — reassign the candidate if needed)"
    )
    for i, t in enumerate(all_turns):
        if t["role"] == "user" and t["kind"] == "text" and t["text"].strip():
            marker = " [STRUCTURAL CANDIDATE]" if i == candidate_idx else ""
            parts.append(f"turn {i}{marker}: {t['text']}")

    assembled = "\n".join(parts)
    return assembled, len(assembled.encode()), candidate_idx, ""


# ---------------------------------------------------------------------------
# LLM parsing
# ---------------------------------------------------------------------------


def _parse_llm_response(response_text: str) -> Optional[Dict[str, Any]]:
    """Parse the classifier's JSON response.

    Returns the parsed dict, or ``None`` if parsing fails. Tolerates
    surrounding whitespace and stripped ```json fences.
    """
    if not response_text:
        return None
    text = response_text.strip()
    # Strip common ```json fences just in case the model ignores the
    # "no markdown fences" instruction.
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return None
    try:
        obj = json.loads(m.group(0))
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    return obj


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _load_extracted_unit_ids(
    exp_conn: sqlite3.Connection, week_start: str
) -> set[str]:
    """Return the set of ``unit_id`` values already in ``expectations`` for the week."""
    rows = exp_conn.execute(
        "SELECT unit_id FROM expectations WHERE week_start = ?",
        (week_start,),
    ).fetchall()
    return {r[0] for r in rows}


def _load_week_units(
    gh_conn: sqlite3.Connection,
    week_start: str,
    repo: Optional[str] = None,
    unit_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[str]:
    """Return all ``unit_id`` values in ``units`` for *week_start*, sorted.

    When *repo* is set (issue #88), filter to units scoped to that repo
    via :func:`synthesis.weekly._repo_filter_sql`. Without it the query
    is byte-identical to the pre-#88 baseline.

    When *unit_ids* is set (issue #90), filter to exactly those ids
    (validated against the DB). When *limit* is set and *unit_ids* is
    not, take the top-N by the same priority order am-synthesize uses.
    """
    fragment, repo_params = _repo_filter_sql(repo, week_start)
    params: List = [week_start, *repo_params]
    rows = gh_conn.execute(
        "SELECT unit_id, abandonment_flag, outlier_flags, elapsed_days "
        f"FROM units WHERE week_start = ?{fragment} "
        "ORDER BY unit_id",
        params,
    ).fetchall()

    all_ids = [r[0] for r in rows]

    if unit_ids:
        known = set(all_ids)
        unknown = [uid for uid in unit_ids if uid not in known]
        if unknown:
            raise ValueError(
                f"Unknown unit_ids for week_start={week_start!r}: {unknown}. "
                "Check that the ids exist in units for this week (and repo if --repo is set)."
            )
        return [uid for uid in all_ids if uid in set(unit_ids)]

    if limit is not None:
        def _priority_key(r: tuple) -> tuple:
            abandoned = 1 if r[1] == 1 else 0
            flags = r[2]
            has_outliers = 1 if (flags is not None and flags != "[]") else 0
            elapsed = r[3] or 0.0
            return (-abandoned, -has_outliers, -elapsed, r[0])
        rows_sorted = sorted(rows, key=_priority_key)
        return [r[0] for r in rows_sorted[:limit]]

    return all_ids


def _store_expectation(
    exp_conn: sqlite3.Connection,
    *,
    week_start: str,
    unit_id: str,
    commitment_point: Optional[str],
    expected_scope: Optional[str],
    expected_effort: Optional[str],
    expected_outcome: Optional[str],
    confidence: Optional[float],
    model: Optional[str],
    input_bytes: int,
    skip_reason: Optional[str],
) -> None:
    """Insert or replace an expectations row."""
    exp_conn.execute(
        "INSERT OR REPLACE INTO expectations "
        "(week_start, unit_id, commitment_point, expected_scope, "
        " expected_effort, expected_outcome, confidence, model, "
        " input_bytes, skip_reason) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            week_start,
            unit_id,
            commitment_point,
            expected_scope,
            expected_effort,
            expected_outcome,
            confidence,
            model,
            input_bytes,
            skip_reason,
        ),
    )
    exp_conn.commit()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_extraction(
    config: SynthesisConfig,
    *,
    github_db: str,
    sessions_db: str,
    expectations_db: str,
    week_start: str,
    rebuild: bool = False,
    repo: Optional[str] = None,
    unit_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> int:
    """Extract expectations for every unit in *week_start*.

    Parameters
    ----------
    config:
        Validated :class:`SynthesisConfig`. Supplies ``model`` (Sonnet)
        and ``anthropic_api_key_env``.
    github_db, sessions_db, expectations_db:
        Filesystem paths to the collector/expectation DBs.
    week_start:
        ``YYYY-MM-DD`` anchor. Must match a value in ``units.week_start``.
    rebuild:
        When True, existing ``expectations`` rows for the week are deleted
        before the pass so every unit is re-extracted.

    Returns
    -------
    ``0`` on success (even if zero units were processed — an empty week is
    a valid no-op). ``1`` if at least one unit raised during the LLM call
    (the run still completes for the rest — no unit is left absent).
    """
    # Pre-flight: ensure both DBs exist with the right schema.
    init_github_db(github_db)
    init_expectations_db(expectations_db)

    adapter = _get_adapter(config)

    # Epic #27 — X-5 (#76): few-shot calibration block. Assembled once
    # per run and prepended to every classifier user message when the
    # ≥20 user-correction threshold is crossed. Below threshold this is
    # an empty string (no-op — identical to X-1..X-4 baseline).
    from synthesis.calibration import build_few_shot_block

    try:
        few_shot_block = build_few_shot_block(expectations_db)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Few-shot calibration block assembly failed (%s); "
            "proceeding without calibration examples.",
            exc,
        )
        few_shot_block = ""

    gh_conn = sqlite3.connect(github_db)
    exp_conn = sqlite3.connect(expectations_db)
    sessions_conn = None
    try:
        _same = sessions_db == github_db
        try:
            if not _same:
                _same = os.path.samefile(github_db, sessions_db)
        except OSError:
            pass
        sessions_conn = gh_conn if _same else sqlite3.connect(sessions_db)

        if rebuild:
            # When rebuild is combined with --repo / --unit-id, clear only
            # the targeted rows. Sub-select against units in github.db via
            # ATTACH is clumsy across two SQLite connections, so we compute
            # the unit_id set in Python and issue a bounded DELETE.
            if unit_ids:
                placeholders = ",".join("?" * len(unit_ids))
                exp_conn.execute(
                    "DELETE FROM expectations WHERE week_start = ? "
                    f"AND unit_id IN ({placeholders})",
                    [week_start] + list(unit_ids),
                )
            elif repo:
                targeted = _load_week_units(gh_conn, week_start, repo=repo)
                if targeted:
                    placeholders = ",".join("?" * len(targeted))
                    exp_conn.execute(
                        "DELETE FROM expectations WHERE week_start = ? "
                        f"AND unit_id IN ({placeholders})",
                        [week_start] + targeted,
                    )
            else:
                exp_conn.execute(
                    "DELETE FROM expectations WHERE week_start = ?",
                    (week_start,),
                )
            exp_conn.commit()

        all_units = _load_week_units(
            gh_conn, week_start, repo=repo, unit_ids=unit_ids, limit=limit
        )
        if not all_units:
            repo_suffix = f" repo={repo}" if repo else ""
            logger.info(
                "No units for week_start=%s%s; nothing to extract",
                week_start, repo_suffix,
            )
            return 0

        already_extracted = _load_extracted_unit_ids(exp_conn, week_start)

        structural_count = 0
        llm_agree_count = 0
        failure_count = 0

        for unit_id in all_units:
            if unit_id in already_extracted:
                logger.debug(
                    "Unit %s already extracted for week %s — skipping",
                    unit_id,
                    week_start,
                )
                continue

            unit_input, input_bytes, candidate_idx, skip_reason = (
                _build_unit_input(gh_conn, sessions_conn, unit_id, week_start)
            )

            logger.info(
                "unit=%s week=%s input_bytes=%d skip_reason=%s",
                unit_id,
                week_start,
                input_bytes,
                skip_reason or "",
            )

            if skip_reason:
                _store_expectation(
                    exp_conn,
                    week_start=week_start,
                    unit_id=unit_id,
                    commitment_point=None,
                    expected_scope=None,
                    expected_effort=None,
                    expected_outcome=None,
                    confidence=None,
                    model=config.model,
                    input_bytes=input_bytes,
                    skip_reason=skip_reason,
                )
                continue

            # Live call — we have a structural candidate and non-empty input.
            structural_count += 1
            # X-5: prepend few-shot calibration block when active. Empty
            # string below threshold means no-op string concatenation.
            classifier_input = (
                f"{few_shot_block}\n{unit_input}" if few_shot_block else unit_input
            )
            try:
                result = adapter.call(
                    _EXPECTATIONS_SYSTEM_PROMPT,
                    classifier_input,
                    config.model,
                    _MAX_OUTPUT_TOKENS,
                )
                parsed = _parse_llm_response(result.text)
            except Exception as exc:
                logger.warning(
                    "Failed to extract expectations for unit %s: %s — "
                    "storing skip row",
                    unit_id,
                    exc,
                )
                failure_count += 1
                _store_expectation(
                    exp_conn,
                    week_start=week_start,
                    unit_id=unit_id,
                    commitment_point=None,
                    expected_scope=None,
                    expected_effort=None,
                    expected_outcome=None,
                    confidence=None,
                    model=config.model,
                    input_bytes=input_bytes,
                    skip_reason="structural_detection_failed_and_llm_declined",
                )
                continue

            if not parsed:
                # LLM declined (unparseable response).
                _store_expectation(
                    exp_conn,
                    week_start=week_start,
                    unit_id=unit_id,
                    commitment_point=None,
                    expected_scope=None,
                    expected_effort=None,
                    expected_outcome=None,
                    confidence=None,
                    model=config.model,
                    input_bytes=input_bytes,
                    skip_reason="structural_detection_failed_and_llm_declined",
                )
                continue

            llm_commit = parsed.get("commitment_point")
            confidence_raw = parsed.get("confidence")
            try:
                confidence = (
                    float(confidence_raw)
                    if confidence_raw is not None
                    else None
                )
            except (ValueError, TypeError):
                confidence = None

            # Agreement heuristic: does the LLM's commitment_point string
            # reference the structural candidate's turn index? The offline
            # FakeAnthropicClient returns a canned Markdown retrospective,
            # not this JSON format — agreement for that path is measured
            # as "LLM returned something parseable that references the
            # candidate index". We treat absence of a parseable JSON as
            # disagreement.
            if (
                isinstance(llm_commit, str)
                and candidate_idx is not None
                and str(candidate_idx) in llm_commit
            ):
                llm_agree_count += 1

            _store_expectation(
                exp_conn,
                week_start=week_start,
                unit_id=unit_id,
                commitment_point=(
                    str(llm_commit) if llm_commit is not None else None
                ),
                expected_scope=parsed.get("expected_scope"),
                expected_effort=parsed.get("expected_effort"),
                expected_outcome=parsed.get("expected_outcome"),
                confidence=confidence,
                model=config.model,
                input_bytes=input_bytes,
                skip_reason=None,
            )

        # Emit the run-level diagnostic summary. This is the signal that
        # surfaces the system-feared failure mode (sparse raw_content_json
        # producing structurally-valid-but-meaningless expectations).
        if structural_count > 0:
            agreement_rate = llm_agree_count / structural_count
        else:
            agreement_rate = 0.0
        logger.info(
            "expectations extraction complete: week=%s units=%d "
            "structural_candidates=%d llm_agreements=%d "
            "agreement_rate=%.3f failures=%d",
            week_start,
            len(all_units),
            structural_count,
            llm_agree_count,
            agreement_rate,
            failure_count,
        )

        if failure_count > 0:
            return 1

    finally:
        if sessions_conn is not None and sessions_conn is not gh_conn:
            sessions_conn.close()
        exp_conn.close()
        gh_conn.close()

    return 0


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am-extract-expectations",
        description=(
            "Extract per-unit expectations (commitment point + scope / "
            "effort / outcome / confidence) for a given week and store "
            "them in expectations.db. Already-extracted units are "
            "skipped unless --rebuild is passed."
        ),
    )
    parser.add_argument(
        "--week",
        required=True,
        help=(
            "Week start date (YYYY-MM-DD). Must match a value in "
            "units.week_start."
        ),
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        default=False,
        help=(
            "Delete existing expectations rows for the week before "
            "re-extracting. Without this flag, already-extracted units "
            "are skipped (idempotent default)."
        ),
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml in repo root).",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help=(
            "Filter to a single repo (owner/name, e.g. "
            "'hyang0129/am-i-shipping'). Only units whose root_node_id "
            "belongs to this repo are extracted. "
            "Intended for dev-loop iteration; expectations remain "
            "partial for non-targeted repos."
        ),
    )
    parser.add_argument(
        "--unit-id",
        dest="unit_ids",
        action="append",
        default=None,
        metavar="UNIT_ID",
        help=(
            "Extract expectations for only this unit_id. Repeatable: "
            "--unit-id A --unit-id B. Takes precedence over --limit. "
            "Errors if any supplied id is absent from units for the week."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Extract expectations for at most N units, selected by the same "
            "priority order am-synthesize uses (abandonment_flag first, then "
            "outlier_flags, then elapsed_days desc). Ignored when --unit-id "
            "is supplied."
        ),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the ``am-extract-expectations`` CLI."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    data_dir = config.data_path
    github_db = str(data_dir / "github.db")
    sessions_db = str(data_dir / "sessions.db")
    expectations_db = str(data_dir / "expectations.db")

    result = run_extraction(
        config.synthesis,
        github_db=github_db,
        sessions_db=sessions_db,
        expectations_db=expectations_db,
        week_start=args.week,
        rebuild=args.rebuild,
        repo=getattr(args, "repo", None),
        unit_ids=getattr(args, "unit_ids", None),
        limit=getattr(args, "limit", None),
    )
    sys.exit(result)


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    main()
