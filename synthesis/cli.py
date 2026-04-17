"""``am-synthesize`` CLI entry point (Epic #17 — Issue #39).

Thin wrapper around :func:`synthesis.weekly.run_synthesis`. The CLI's
job is to parse flags and resolve DB paths from ``config.yaml``; the
synthesis pipeline itself lives in ``synthesis.weekly``.

Usage::

    am-synthesize --week 2026-04-12
    am-synthesize --week 2026-04-12 --dry-run
    AMIS_SYNTHESIS_LIVE=1 ANTHROPIC_API_KEY=sk-... am-synthesize --week 2026-04-12

When ``AMIS_SYNTHESIS_LIVE`` is unset, the run uses the offline
:class:`synthesis.fake_client.FakeAnthropicClient` and does not require
an API key.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Optional, Sequence

from am_i_shipping.config_loader import load_config
from synthesis.weekly import run_synthesis


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am-synthesize",
        description=(
            "Run the weekly synthesis engine for a given week_start. "
            "Writes retrospectives/<week>.md. Use --dry-run to emit the "
            "assembled prompt without calling the API."
        ),
    )
    parser.add_argument(
        "--week",
        required=True,
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
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point. Returns a shell-suitable exit code.

    Exit codes
    ----------
    * ``0`` — retrospective written, dry-run prompt written, the file
      already existed (idempotent success), or no units were found for
      the requested week. All of these are non-error conditions.
    * ``2`` — unexpected error (traceback logged).

    The "no units for the week" and "file already exists" cases both
    collapse to exit 0 because re-running ``am-synthesize --week
    <same-week>`` is expected to be a cheap no-op per ADR Decision 2
    (idempotency). A human driver will notice the stderr message
    ``No retrospective written (...)`` when the run produced nothing.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    # Resolve DB paths against the config's data directory. ``data_path``
    # on the Config returns an absolute Path relative to the config file
    # so the CLI works regardless of the caller's cwd.
    data_dir: Path = config.data_path
    github_db = data_dir / "github.db"
    sessions_db = data_dir / "sessions.db"

    # Resolve the synthesis output dir the same way — via the Config's
    # own property, which anchors relative paths against the config
    # file's directory (NOT against ``data_dir.parent``, which only
    # coincided with the config dir for the default ``data_dir="data"``
    # layout). See Config.synthesis_output_path.
    synthesis_cfg = config.synthesis
    output_dir = config.synthesis_output_path
    # Mutate via a fresh dataclass copy so we don't silently rewrite
    # the caller's config object — replace only the resolved path.
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
        # Two cases reach here: (a) no units for the week, (b) the
        # retrospective already existed and we skipped. Both are
        # non-errors — idempotent success for case (b), and no-op for
        # case (a). We log INFO in both cases inside the pipeline so
        # the CLI simply surfaces a zero exit.
        print(
            "No retrospective written (no units for week, or file already exists)",
            file=sys.stderr,
        )
        return 0

    if result is not None:
        print(str(result))
    return 0


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    sys.exit(main())
