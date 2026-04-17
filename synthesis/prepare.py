"""``am-prepare-week`` CLI entry point (Epic #50 — Issue #52).

Thin orchestration layer over the three Phase 2 pipeline stages that
must run before :func:`synthesis.weekly.run_synthesis` has anything to
read. The stages are already implemented and idempotent on their own;
this module exists solely so the pipeline has one command to run per
week instead of three Python imports.

Usage::

    am-prepare-week --week 2026-04-13
    am-prepare-week --week 2026-04-13 --config /path/to/config.yaml

The command calls, in order:

1. :func:`synthesis.graph_builder.build_graph`
   (populates ``graph_nodes`` + ``graph_edges`` for the week)
2. :func:`synthesis.unit_identifier.identify_units`
   (populates ``units`` for the week)
3. :func:`synthesis.cross_unit.compute_flags`
   (fills ``outlier_flags`` + ``abandonment_flag`` on ``units``)

After it finishes, ``am-synthesize --week <same-week>`` is able to run
without any manual Python orchestration — the precondition it used to
fail on (empty ``units`` table) is now satisfied.

Idempotency
-----------
Every underlying stage is idempotent by design: ``build_graph`` and
``identify_units`` both use ``INSERT OR IGNORE`` keyed on the natural
IDs, and ``compute_flags`` overwrites its two flag columns in place.
Running ``am-prepare-week`` twice in a row therefore produces no new
rows and no new side effects past the first call.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Sequence

from am_i_shipping.config_loader import load_config
from synthesis.cross_unit import compute_flags
from synthesis.graph_builder import build_graph
from synthesis.unit_identifier import identify_units


logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am-prepare-week",
        description=(
            "Orchestrate the Phase 2 preparation pipeline for a given "
            "week_start: build_graph -> identify_units -> compute_flags. "
            "Run before am-synthesize so the units table is populated."
        ),
    )
    parser.add_argument(
        "--week",
        required=True,
        help="Week start date (YYYY-MM-DD). Written to graph/unit partitions.",
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
    * ``0`` — all three stages completed, even if one stage produced
      zero new rows. An empty week is a valid state: ``identify_units``
      returning 0 means the collector DB has no graph nodes for
      ``--week``, which is a "nothing to do" signal rather than a
      failure.
    * ``2`` — unexpected error (traceback logged). Mirrors the
      :mod:`synthesis.cli` convention so callers (``run_collectors.sh``)
      treat prepare and synthesize failures the same way.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    # Resolve DB paths the same way synthesis/cli.py does so the two
    # CLIs agree on which files they touch. See Config.data_path for
    # the relative-path anchoring rule (resolved against the directory
    # that holds config.yaml, not the caller's cwd).
    data_dir: Path = config.data_path
    github_db = data_dir / "github.db"
    sessions_db = data_dir / "sessions.db"
    week = args.week
    # Thread the user's synthesis config through to the two stages that
    # expose tunables. build_graph has no such knobs — it emits the
    # full graph for the week unconditionally.
    synthesis_cfg = config.synthesis

    try:
        logger.info("Stage 1/3: build_graph week=%s", week)
        build_graph(sessions_db, github_db, week_start=week)

        logger.info("Stage 2/3: identify_units week=%s", week)
        # identify_units returns rows inserted (0 is valid — empty
        # weeks pass through as a no-op thanks to INSERT OR IGNORE).
        units_inserted = identify_units(
            github_db,
            sessions_db,
            week,
            abandonment_days=synthesis_cfg.abandonment_days,
        )
        logger.info("identify_units inserted %d row(s)", units_inserted)

        logger.info("Stage 3/3: compute_flags week=%s", week)
        # compute_flags returns rows updated — equal to the current
        # population of units for the week. 0 here just means the
        # previous stage wrote nothing.
        flags_updated = compute_flags(
            github_db,
            week,
            outlier_sigma=synthesis_cfg.outlier_sigma,
            abandonment_days=synthesis_cfg.abandonment_days,
        )
        logger.info("compute_flags updated %d row(s)", flags_updated)
    except Exception:  # noqa: BLE001 — CLI is the top of the stack
        logging.exception("am-prepare-week failed")
        return 2

    return 0


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    sys.exit(main())
