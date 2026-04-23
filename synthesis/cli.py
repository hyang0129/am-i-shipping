"""``am-synthesize`` CLI entry point.

Thin wrapper around the synthesis pipeline and the coverage diagnostic.
Two invocation shapes are supported:

Weekly synthesis (Epic #17 — Issue #39; the original command)::

    am-synthesize --week 2026-04-12
    am-synthesize --week 2026-04-12 --dry-run

Coverage diagnostic (Issue #70 — pre-synthesis health check)::

    am-synthesize coverage
    am-synthesize coverage --json
    am-synthesize coverage --backfill
    am-synthesize coverage --backfill --full

The bare ``--week`` form is preserved verbatim so scripts and cron entries
that predate the coverage subcommand continue to work — no breaking change.

When ``AMIS_SYNTHESIS_LIVE`` is unset, weekly runs use the offline
:class:`synthesis.fake_client.FakeAnthropicClient` and do not require an API
key.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Optional, Sequence

from am_i_shipping.config_loader import load_config
from synthesis.coverage import add_coverage_subparser, run_coverage
from synthesis.weekly import run_synthesis


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am-synthesize",
        description=(
            "Run the weekly synthesis engine for a given week_start, or the "
            "coverage diagnostic (pre-synthesis health check). "
            "Weekly: writes retrospectives/<week>.md. "
            "Coverage: reports raw_content_json fill state across sessions.db "
            "and JSONL files on disk."
        ),
    )
    # Top-level ``--week`` / ``--dry-run`` / ``--config`` are retained for
    # backward compatibility with ``am-synthesize --week YYYY-MM-DD``. When a
    # subcommand is used they are ignored.
    parser.add_argument(
        "--week",
        default=None,
        help="Week start date (YYYY-MM-DD). Must match units.week_start.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Write the assembled prompt to retrospectives/.dry-run/ "
            "instead of calling the API."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: config.yaml in repo root).",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help=(
            "Filter LLM stages to a single repo (owner/name, e.g. "
            "'hyang0129/am-i-shipping'). Writes the retrospective to "
            "retrospectives/<week>/<owner>__<name>.md so single-repo "
            "and full-weekly runs for the same week coexist. "
            "Intended for dev-loop iteration; unit_summaries and "
            "expectations remain partial for non-targeted repos."
        ),
    )
    parser.add_argument(
        "--unit-id",
        dest="unit_ids",
        action="append",
        default=None,
        metavar="UNIT_ID",
        help=(
            "Restrict all LLM stages to this unit_id. Repeatable: "
            "--unit-id A --unit-id B. Takes precedence over --limit. "
            "Useful when unit_summaries exist for only a subset of the week."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Restrict all LLM stages to at most N units, selected by the "
            "same priority order as the internal truncation (abandonment_flag "
            "first, then outlier_flags, then elapsed_days desc). Ignored "
            "when --unit-id is supplied."
        ),
    )

    subparsers = parser.add_subparsers(dest="subcommand")
    add_coverage_subparser(subparsers)
    _add_correct_subparser(subparsers)
    return parser


def _add_correct_subparser(subparsers) -> None:
    """Register the ``am-synthesize correct`` subcommand (Epic #27 X-4)."""
    correct = subparsers.add_parser(
        "correct",
        help=(
            "Interactive agentic correction loop for the week's major/"
            "critical expectation gaps. Persists corrections into "
            "expectations.db; does NOT rewrite retrospective .md files."
        ),
    )
    correct.add_argument(
        "--week",
        default=None,
        help=(
            "Week start date (YYYY-MM-DD). Default: most recent week that "
            "has gap rows in expectations.db."
        ),
    )
    correct.add_argument(
        "--unit",
        default=None,
        help=(
            "Restrict the correction loop to a single unit_id. Default: "
            "iterate over every major/critical gap in the week."
        ),
    )


def _run_weekly(args: argparse.Namespace) -> int:
    """Preserve the original ``--week``-driven synthesis entry point."""
    if not args.week:
        print(
            "am-synthesize: either use a subcommand (e.g. 'coverage') "
            "or pass --week YYYY-MM-DD for weekly synthesis.",
            file=sys.stderr,
        )
        return 2

    config = load_config(args.config)
    # Resolve DB paths against the config's data directory. ``data_path`` on
    # the Config returns an absolute Path relative to the config file so the
    # CLI works regardless of the caller's cwd.
    data_dir: Path = config.data_path
    github_db = data_dir / "github.db"
    sessions_db = data_dir / "sessions.db"
    # Epic #27 — X-2 (#73): expectations.db hosts both X-1 expectation rows
    # and the X-2 expectation_gaps table. Path only — existence is not
    # required; ``run_synthesis`` handles the missing-DB case gracefully.
    expectations_db = data_dir / "expectations.db"

    synthesis_cfg = config.synthesis
    output_dir = config.synthesis_output_path
    resolved_cfg = replace(synthesis_cfg, output_dir=str(output_dir))

    try:
        result = run_synthesis(
            resolved_cfg,
            github_db,
            sessions_db,
            args.week,
            dry_run=args.dry_run,
            expectations_db=expectations_db,
            repo=getattr(args, "repo", None),
            unit_ids=getattr(args, "unit_ids", None),
            limit=getattr(args, "limit", None),
        )
    except Exception:  # noqa: BLE001 — CLI is the top of the stack
        logging.exception("Synthesis failed")
        return 2

    if result is None and not args.dry_run:
        repo_suffix = (
            f" for repo={args.repo!r}" if getattr(args, "repo", None) else ""
        )
        print(
            "No retrospective written (no units for week, or file already "
            f"exists){repo_suffix}",
            file=sys.stderr,
        )
        return 0

    if result is not None:
        print(str(result))
    return 0


def _latest_week_with_gaps(expectations_db: Path) -> Optional[str]:
    """Return the most recent ``week_start`` that has any gap rows."""
    import sqlite3

    if not expectations_db.exists():
        return None
    conn = sqlite3.connect(str(expectations_db))
    try:
        try:
            row = conn.execute(
                "SELECT week_start FROM expectation_gaps "
                "ORDER BY week_start DESC LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return row[0] if row else None
    finally:
        conn.close()


def _run_correct(args: argparse.Namespace) -> int:
    """``am-synthesize correct`` — X-4 interactive correction loop."""
    config = load_config(args.config)
    data_dir: Path = config.data_path
    expectations_db = data_dir / "expectations.db"

    week = args.week or _latest_week_with_gaps(expectations_db)
    if not week:
        print(
            "am-synthesize correct: no week specified and no gap rows found "
            "in expectations.db. Run 'am-synthesize --week YYYY-MM-DD' first.",
            file=sys.stderr,
        )
        return 2

    synthesis_cfg = config.synthesis
    output_dir = config.synthesis_output_path
    resolved_cfg = replace(synthesis_cfg, output_dir=str(output_dir))

    try:
        from synthesis.correction import run_correction_session

        written = run_correction_session(
            week,
            expectations_db=str(expectations_db),
            config=resolved_cfg,
            unit_id=args.unit,
        )
    except Exception:  # noqa: BLE001 — CLI is top of stack
        logging.exception("Correction session failed")
        return 2

    print(f"Correction session complete: {written} rows written for week={week}")
    return 0


def _run_coverage(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    data_dir: Path = config.data_path
    sessions_db = data_dir / "sessions.db"
    projects_path = Path(config.session.projects_path)
    return run_coverage(
        sessions_db=sessions_db,
        projects_path=projects_path,
        week_start=config.synthesis.week_start,
        data_dir=data_dir,
        emit_json=bool(getattr(args, "emit_json", False)),
        do_backfill=bool(getattr(args, "backfill", False)),
        full_rebuild=bool(getattr(args, "full", False)),
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point. Returns a shell-suitable exit code.

    Exit codes
    ----------
    * ``0`` — retrospective written, dry-run prompt written, the file already
      existed (idempotent success), no units were found for the requested
      week, or coverage report completed. All of these are non-error.
    * ``2`` — unexpected error (traceback logged) or invalid argument combo.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.subcommand == "coverage":
        return _run_coverage(args)
    if args.subcommand == "correct":
        return _run_correct(args)
    return _run_weekly(args)


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    sys.exit(main())
