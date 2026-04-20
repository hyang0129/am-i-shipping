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

    subparsers = parser.add_subparsers(dest="subcommand")
    add_coverage_subparser(subparsers)
    return parser


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
        )
    except Exception:  # noqa: BLE001 — CLI is the top of the stack
        logging.exception("Synthesis failed")
        return 2

    if result is None and not args.dry_run:
        print(
            "No retrospective written (no units for week, or file already exists)",
            file=sys.stderr,
        )
        return 0

    if result is not None:
        print(str(result))
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
    return _run_weekly(args)


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    sys.exit(main())
