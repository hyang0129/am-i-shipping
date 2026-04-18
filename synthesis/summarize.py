"""``am-summarize-units`` CLI — per-unit narrative summarization (Issue #64).

For each unit in the ``units`` table that lacks a ``unit_summaries`` row,
calls the Anthropic API (or the offline :class:`FakeAnthropicClient`) to
produce a 150–300 word prose narrative covering the unit's development arc:
initial intent, collaboration pattern, and execution/outcome.

Summary results are persisted in ``unit_summaries`` (github.db). The CLI
is idempotent by default — already-summarized units are skipped. Pass
``--rebuild-summaries`` to wipe the week's existing summaries and
regenerate all of them.

Architecture decisions
----------------------
* **Offline/live client selection** mirrors :func:`synthesis.llm_adapter._get_adapter`:
  ``AMIS_SYNTHESIS_LIVE=1`` enables the live SDK; anything else uses
  :class:`FakeAnthropicClient`.
* **Prompt caching**: the static system prompt is sent with
  ``cache_control: ephemeral`` in live mode so repeated runs pay a
  cache-read price on the boilerplate.
* **Reuse**: :func:`synthesis.weekly._unit_nodes` and
  :func:`synthesis.weekly._load_session_transcripts` are imported
  directly — the graph-walk and session-lookup logic is identical, and
  maintaining a copy would be a maintenance hazard.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from typing import List, Optional, Sequence, Tuple

from am_i_shipping.config_loader import SynthesisConfig, load_config
from am_i_shipping.db import init_github_db
from synthesis.llm_adapter import _get_adapter
from synthesis.weekly import _load_session_transcripts, _unit_nodes


logger = logging.getLogger(__name__)


# Upper bound on output tokens for a single unit summary call. A narrative
# of 150–300 words is roughly 200–400 tokens; 1024 is generous headroom.
_MAX_OUTPUT_TOKENS = 1024

_SUMMARY_SYSTEM_PROMPT = """You are summarizing a software development unit for a weekly retrospective.

A "unit" is one complete development cycle: from GitHub issue creation through PR merge or abandonment.

Produce a 150–300 word prose narrative (story arc) covering:
1. Initial intent — how clearly was the issue scoped? Was acceptance criteria stated upfront?
2. Collaboration pattern — did the user drop one prompt and let Claude run autonomously, or reprompt repeatedly? Was there a design phase before execution?
3. Execution and outcome — how did the PR land (clean merge, review cycles, abandoned)? Did the actual outcome match the stated intent?

Write in third-person narrative. Cite specific filenames, PR numbers, or issue text where they clarify the signal.
Do NOT prescribe follow-ups. Do NOT invent signals not present in the inputs. Do NOT copy raw transcript text verbatim.
"""


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _load_unsummarized_units(
    gh_conn: sqlite3.Connection,
    week_start: str,
    rebuild: bool = False,
) -> List[dict]:
    """Return units for *week_start* that do not yet have a summary row.

    If *rebuild* is True, existing ``unit_summaries`` rows for the week
    are deleted before the query so every unit is treated as unsummarized.
    """
    if rebuild:
        gh_conn.execute(
            "DELETE FROM unit_summaries WHERE week_start = ?",
            (week_start,),
        )
        gh_conn.commit()

    rows = gh_conn.execute(
        "SELECT u.unit_id FROM units u "
        "LEFT JOIN unit_summaries s "
        "  ON u.week_start = s.week_start AND u.unit_id = s.unit_id "
        "WHERE u.week_start = ? AND s.unit_id IS NULL "
        "ORDER BY u.unit_id",
        (week_start,),
    ).fetchall()
    return [{"unit_id": r[0]} for r in rows]


def _build_unit_input(
    gh_conn: sqlite3.Connection,
    sessions_conn: sqlite3.Connection,
    unit_id: str,
    week_start: str,
) -> Tuple[str, int]:
    """Assemble the LLM input string for *unit_id* and return ``(text, input_bytes)``.

    Combines:
    - Unit metrics row from ``units``
    - Issue title/body for any issue nodes in the unit's graph component
    - PR title/body for any PR nodes in the unit's graph component
    - Session transcripts (raw, untruncated — these are per-unit calls, not
      the whole-week water-fill budget used by :func:`synthesis.weekly.run_synthesis`)

    Returns ``("(no session transcripts for this unit)", 0)`` when the
    unit has no session nodes (or all session nodes have NULL transcripts).
    """
    # Fetch the unit metrics row.
    unit_row = gh_conn.execute(
        "SELECT unit_id, root_node_type, root_node_id, "
        "       elapsed_days, dark_time_pct, total_reprompts, "
        "       review_cycles, status, outlier_flags, abandonment_flag "
        "FROM units WHERE week_start = ? AND unit_id = ?",
        (week_start, unit_id),
    ).fetchone()

    parts: List[str] = []

    if unit_row:
        parts.append(f"## Unit: {unit_row[0]}")
        parts.append(f"- root_node_type: {unit_row[1]}")
        parts.append(f"- root_node_id: {unit_row[2]}")
        parts.append(f"- elapsed_days: {unit_row[3]}")
        parts.append(f"- dark_time_pct: {unit_row[4]}")
        parts.append(f"- total_reprompts: {unit_row[5]}")
        parts.append(f"- review_cycles: {unit_row[6]}")
        parts.append(f"- status: {unit_row[7]}")
        parts.append(f"- outlier_flags: {unit_row[8] or '[]'}")
        abandoned = unit_row[9]
        parts.append(
            f"- abandonment_flag: "
            f"{'yes' if abandoned == 1 else ('no' if abandoned == 0 else 'n/a')}"
        )
        root_node_id = unit_row[2] or ""
    else:
        root_node_id = ""

    # Walk the graph to collect the component.
    component = _unit_nodes(gh_conn, week_start, root_node_id)

    # Collect issue and PR nodes to fetch their text.
    issue_node_refs: List[str] = []
    pr_node_refs: List[str] = []
    session_uuids: List[str] = []

    for _nid, node_type, node_ref in component:
        if node_type == "issue" and node_ref:
            issue_node_refs.append(node_ref)
        elif node_type == "pr" and node_ref:
            pr_node_refs.append(node_ref)
        elif node_type == "session" and node_ref:
            session_uuids.append(node_ref)

    # Fetch issue text via the ``issues`` table.
    for node_ref in issue_node_refs:
        # node_ref shape: "issue:<repo>#<number>" or just "<repo>#<number>"
        # Strip a leading "issue:" prefix if present.
        ref = node_ref.removeprefix("issue:")
        if "#" in ref:
            repo, _, num_str = ref.rpartition("#")
            try:
                issue_number = int(num_str)
            except ValueError:
                continue
            row = gh_conn.execute(
                "SELECT title, body FROM issues WHERE repo = ? AND issue_number = ?",
                (repo, issue_number),
            ).fetchone()
            if row:
                title, body = row
                parts.append(f"\n### Issue: {node_ref}")
                if title:
                    parts.append(f"**Title**: {title}")
                if body:
                    parts.append(f"**Body**:\n{body}")

    # Fetch PR text via the ``pull_requests`` table.
    for node_ref in pr_node_refs:
        ref = node_ref.removeprefix("pr:")
        if "#" in ref:
            repo, _, num_str = ref.rpartition("#")
            try:
                pr_number = int(num_str)
            except ValueError:
                continue
            row = gh_conn.execute(
                "SELECT title, body FROM pull_requests WHERE repo = ? AND pr_number = ?",
                (repo, pr_number),
            ).fetchone()
            if row:
                title, body = row
                parts.append(f"\n### PR: {node_ref}")
                if title:
                    parts.append(f"**Title**: {title}")
                if body:
                    parts.append(f"**Body**:\n{body}")

    # Fetch session transcripts.
    transcripts = _load_session_transcripts(sessions_conn, session_uuids)
    non_empty = [(uid, content) for uid, content in transcripts if content]

    if not non_empty:
        return ("(no session transcripts for this unit)", 0)

    parts.append("\n### Session Transcripts")
    for uid, content in non_empty:
        parts.append(f"\n#### Session {uid}")
        parts.append(content)

    assembled = "\n".join(parts)
    return assembled, len(assembled.encode())


def _summarize_unit(
    config: SynthesisConfig,
    unit_input: str,
) -> str:
    """Call the LLM to produce a prose narrative for one unit."""
    result = _get_adapter(config).call(
        _SUMMARY_SYSTEM_PROMPT,
        unit_input,
        config.summary_model,
        _MAX_OUTPUT_TOKENS,
    )
    return result.text


def _store_summary(
    gh_conn: sqlite3.Connection,
    week_start: str,
    unit_id: str,
    summary_text: str,
    model: str,
    input_bytes: int,
) -> None:
    """Persist a unit summary row, replacing any existing row for the same key."""
    gh_conn.execute(
        "INSERT OR REPLACE INTO unit_summaries "
        "(week_start, unit_id, summary_text, model, input_bytes) "
        "VALUES (?, ?, ?, ?, ?)",
        (week_start, unit_id, summary_text, model, input_bytes),
    )
    gh_conn.commit()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_summarization(
    config: SynthesisConfig,
    github_db: str,
    sessions_db: str,
    week_start: str,
    rebuild: bool = False,
) -> int:
    """Summarize all unsummarized units for *week_start*.

    Parameters
    ----------
    config:
        Validated :class:`SynthesisConfig`. Supplies ``summary_model`` and
        ``anthropic_api_key_env``.
    github_db, sessions_db:
        Filesystem paths to the collector DBs.
    week_start:
        ``YYYY-MM-DD`` anchor. Must match a value in ``units.week_start``.
    rebuild:
        When ``True``, existing ``unit_summaries`` rows for the week are
        deleted before the pass so every unit is re-summarized.

    Returns
    -------
    ``0`` on success (even if zero units were processed — an empty week is
    a valid no-op state).
    """
    # Pre-flight: ensure unit_summaries table exists in github.db.
    # init_github_db is idempotent (CREATE TABLE IF NOT EXISTS).
    init_github_db(github_db)


    sessions_conn = None
    gh_conn = sqlite3.connect(github_db)
    try:
        _same = sessions_db == github_db
        try:
            if not _same:
                import os as _os
                _same = _os.path.samefile(github_db, sessions_db)
        except OSError:
            pass
        sessions_conn = gh_conn if _same else sqlite3.connect(sessions_db)

        try:
            units = _load_unsummarized_units(gh_conn, week_start, rebuild=rebuild)

            if not units:
                logger.info(
                    "No unsummarized units for week_start=%s; nothing to do",
                    week_start,
                )
                return 0

            for unit in units:
                unit_id = unit["unit_id"]
                unit_input, input_bytes = _build_unit_input(
                    gh_conn, sessions_conn, unit_id, week_start
                )
                try:
                    summary_text = _summarize_unit(config, unit_input)
                except Exception as exc:
                    logger.warning(
                        "Failed to summarize unit %s: %s — skipping", unit_id, exc
                    )
                    continue
                _store_summary(
                    gh_conn,
                    week_start,
                    unit_id,
                    summary_text,
                    config.summary_model,
                    input_bytes,
                )
                logger.info("Summarized unit %s (week_start=%s)", unit_id, week_start)

                word_count = len(summary_text.split())
                if word_count < 100 or word_count > 400:
                    logger.warning(
                        "Summary for unit %s has %d words (expected 100–400)",
                        unit_id,
                        word_count,
                    )

        finally:
            if sessions_conn is not None and sessions_conn is not gh_conn:
                sessions_conn.close()

    finally:
        gh_conn.close()

    return 0


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am-summarize-units",
        description=(
            "Produce per-unit narrative summaries for a given week and store "
            "them in the unit_summaries table of github.db. "
            "Already-summarized units are skipped unless --rebuild-summaries "
            "is passed."
        ),
    )
    parser.add_argument(
        "--week",
        required=True,
        help="Week start date (YYYY-MM-DD). Must match a value in units.week_start.",
    )
    parser.add_argument(
        "--rebuild-summaries",
        action="store_true",
        default=False,
        help=(
            "Delete existing unit_summaries rows for the week before "
            "regenerating. Without this flag, already-summarized units are "
            "skipped (idempotent default)."
        ),
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml in repo root).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the ``am-summarize-units`` CLI."""
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

    result = run_summarization(
        config.synthesis,
        github_db=github_db,
        sessions_db=sessions_db,
        week_start=args.week,
        rebuild=args.rebuild_summaries,
    )
    sys.exit(result)


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    main()
