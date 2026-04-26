"""``am-rebuild-history`` CLI entry point — Epic #93, Slice 3 (Issue #104).

Historical rebuild: drop every unit_id-keyed and cross-week graph table,
take a SQLite backup before doing so, then iterate every distinct
``week_start`` and rebuild the synthesis pipeline (build_graph →
identify_units → compute_flags) under the new directional-edge model.

This complements ``am-prepare-week``, which is single-week only. Use
this command after Slice 2 lands new edge writers — re-running the
rebuild against existing DBs regenerates the graph (and unit_id hashes)
under the new vocabulary.

Decisions inherited from the epic intent:

* Decision 3 (EXPENSIVE-TO-REVERSE) — drop and rebuild every downstream
  table keyed by old ``unit_id``. ``expectation_corrections`` is
  user-authored and destroyed; we surface the destroyed row count
  loudly to honour the "not silent" requirement from the slice spec.
* Section 7 rollback posture — atomic across weeks. If the rebuild
  fails partway, restore both DBs from the backup taken before the
  rebuild begins and re-raise.

What this module does NOT do:

* Re-run ``am-summarize-units``, ``am-extract-expectations``, or
  ``am-synthesize correct``. Those are invoked separately; they read
  from the rebuilt ``units`` table on demand.
* Preserve old ``unit_id`` values via a mapping table (Anti-choice 5).
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence

from am_i_shipping.config_loader import load_config
from am_i_shipping.db import init_expectations_db, init_github_db
from synthesis.cross_unit import compute_flags
from synthesis.graph_builder import build_graph
from synthesis.unit_identifier import identify_units


logger = logging.getLogger(__name__)


# Tables whose every row is keyed by ``unit_id`` (or whose every row is
# keyed by ``week_start`` and is regenerated as a side effect of the
# rebuild). Order is the safe drop order — none of these have explicit
# foreign keys today, but ``unit_summaries`` declares a soft FK against
# ``units`` which we honour by dropping the dependent first.
GITHUB_DROP_TABLES: tuple[str, ...] = (
    "unit_summaries",
    "units",
    "session_issue_attribution",
    "graph_edges",
    "graph_nodes",
)

EXPECTATIONS_DROP_TABLES: tuple[str, ...] = (
    # ``expectation_corrections`` first because it is the user-authored
    # data we need to count before destruction.
    "expectation_corrections",
    "expectation_revisions",
    "expectation_gaps",
    "expectations",
    # ``expectation_calibration_trends`` is computed from corrections;
    # drop it too so it does not retain stale work-type rows after
    # corrections vanish (Slice 3 spec, Open Question Q1 default = yes).
    "expectation_calibration_trends",
)


# ---------------------------------------------------------------------------
# Backup / restore
# ---------------------------------------------------------------------------


def _backup_db(src: Path, dst: Path) -> None:
    """Take a consistent SQLite backup of *src* to *dst*.

    Uses the SQLite online-backup API (``Connection.backup``) rather
    than a file copy so concurrent writers (none expected during
    rebuild, but cheap insurance) cannot leave a torn file.
    """
    if not src.exists():
        # Nothing to back up — the rebuild may legitimately run against
        # a DB that does not yet exist (init_*_db will create it).
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    src_conn = sqlite3.connect(str(src))
    try:
        dst_conn = sqlite3.connect(str(dst))
        try:
            src_conn.backup(dst_conn)
        finally:
            dst_conn.close()
    finally:
        src_conn.close()


def _restore_db(backup: Path, target: Path) -> None:
    """Restore *target* from *backup* by file copy.

    ``Connection.backup`` would also work in reverse, but a plain copy
    is simpler and we are guaranteed no live connections remain at
    restore time (the rebuild has already raised).
    """
    if not backup.exists():
        # No backup taken (source DB did not exist at backup time);
        # nothing to restore. Mirror that state by removing target.
        if target.exists():
            target.unlink()
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(backup, target)


# ---------------------------------------------------------------------------
# Per-DB drop helpers
# ---------------------------------------------------------------------------


def _count_corrections(expectations_db: Path) -> int:
    """Return the number of ``expectation_corrections`` rows about to die.

    Returns 0 if the file does not exist or the table is missing —
    those are valid pre-rebuild states and are not a reason to fail.
    """
    if not expectations_db.exists():
        return 0
    with closing(sqlite3.connect(str(expectations_db))) as conn:
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM expectation_corrections"
            ).fetchone()[0]
        except sqlite3.OperationalError:
            return 0


def _drop_tables(db_path: Path, tables: Iterable[str]) -> None:
    """``DROP TABLE IF EXISTS`` every table in *tables*, in order."""
    if not db_path.exists():
        return
    with closing(sqlite3.connect(str(db_path))) as conn:
        for table in tables:
            if not table.isidentifier():
                # Defence-in-depth — these come from a module constant
                # today, but we never want a non-identifier reaching
                # the f-string below.
                raise ValueError(f"Refusing to DROP non-identifier {table!r}")
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()


def _distinct_weeks(github_db: Path) -> list[str]:
    """Return the sorted list of distinct ``week_start`` values.

    Reads from the *backup* of ``graph_nodes`` (the source of truth
    for which weeks have ever been ingested) before the live DB has
    its tables dropped. Caller passes the backup path.
    """
    if not github_db.exists():
        return []
    with closing(sqlite3.connect(str(github_db))) as conn:
        try:
            rows = conn.execute(
                "SELECT DISTINCT week_start FROM graph_nodes "
                "WHERE week_start IS NOT NULL "
                "ORDER BY week_start"
            ).fetchall()
        except sqlite3.OperationalError:
            return []
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def rebuild_history(
    github_db: Path,
    sessions_db: Path,
    expectations_db: Path,
    *,
    abandonment_days: int,
    outlier_sigma: float,
    backup_dir: Optional[Path] = None,
    weeks: Optional[Sequence[str]] = None,
) -> dict:
    """Drop and rebuild every unit_id-keyed table across all weeks.

    Parameters
    ----------
    github_db, sessions_db, expectations_db:
        Filesystem paths to the three DBs.
    abandonment_days, outlier_sigma:
        Forwarded to ``compute_flags`` (matches ``am-prepare-week``
        behaviour exactly so consumers see no semantic drift in
        ``units.outlier_flags`` / ``abandonment_flag`` between the
        two CLIs).
    backup_dir:
        Where to write the pre-rebuild ``.bak`` copies. Defaults to
        ``<github_db.parent>/_rebuild_backup_<timestamp>/``.
    weeks:
        Optional override — if provided, iterate exactly these weeks
        instead of ``SELECT DISTINCT week_start FROM graph_nodes``.
        Useful for tests and for partial rebuilds.

    Returns
    -------
    dict
        Summary dict with keys ``backup_dir``, ``weeks``,
        ``corrections_destroyed``.

    Raises
    ------
    Any exception from the rebuild stages — the caller (``main``)
    catches and turns into a non-zero exit code.
    """
    if backup_dir is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_dir = github_db.parent / f"_rebuild_backup_{ts}"

    backup_dir.mkdir(parents=True, exist_ok=True)
    gh_backup = backup_dir / "github.db.bak"
    sess_backup = backup_dir / "sessions.db.bak"
    exp_backup = backup_dir / "expectations.db.bak"

    logger.info("Pre-rebuild backup -> %s", backup_dir)
    _backup_db(github_db, gh_backup)
    _backup_db(sessions_db, sess_backup)
    _backup_db(expectations_db, exp_backup)

    # LOUD per Decision 3: count user-authored corrections before drop.
    corrections_destroyed = _count_corrections(expectations_db)
    if corrections_destroyed > 0:
        logger.warning(
            "Dropping `expectation_corrections` (%d rows of user-authored "
            "corrections; these are not recoverable). Decision 3 / Issue #104.",
            corrections_destroyed,
        )
    else:
        logger.info(
            "No expectation_corrections rows to destroy (Decision 3)."
        )

    # Resolve weeks BEFORE drop — once graph_nodes is gone we cannot
    # recover the historical week list from the live DB.
    if weeks is None:
        resolved_weeks = _distinct_weeks(github_db)
    else:
        resolved_weeks = list(weeks)

    logger.info(
        "Rebuild plan: %d week(s): %s",
        len(resolved_weeks),
        ", ".join(resolved_weeks) if resolved_weeks else "(none)",
    )

    try:
        # Atomic per-DB drop. If init_*_db fails after this we restore
        # everything from backup; the live state is otherwise empty
        # of the dropped tables.
        _drop_tables(github_db, GITHUB_DROP_TABLES)
        _drop_tables(expectations_db, EXPECTATIONS_DROP_TABLES)

        # Recreate the schemas (idempotent — also picks up Slice 1's
        # ``traversal`` column migration on legacy DBs).
        init_github_db(github_db)
        init_expectations_db(expectations_db)

        for idx, week in enumerate(resolved_weeks, start=1):
            logger.info(
                "[%d/%d] Rebuilding week_start=%s",
                idx,
                len(resolved_weeks),
                week,
            )
            build_graph(sessions_db, github_db, week_start=week)
            identify_units(
                github_db,
                sessions_db,
                week,
                abandonment_days=abandonment_days,
            )
            compute_flags(
                github_db,
                week,
                outlier_sigma=outlier_sigma,
                abandonment_days=abandonment_days,
            )
    except Exception:
        logger.exception(
            "Rebuild failed — restoring DBs from backup at %s", backup_dir
        )
        try:
            _restore_db(gh_backup, github_db)
            _restore_db(sess_backup, sessions_db)
            _restore_db(exp_backup, expectations_db)
        except Exception:  # pragma: no cover — restore failure is rare
            logger.exception("Restore from backup ALSO failed — manual recovery required")
        raise

    return {
        "backup_dir": backup_dir,
        "weeks": resolved_weeks,
        "corrections_destroyed": corrections_destroyed,
    }


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am-rebuild-history",
        description=(
            "Drop and rebuild every unit_id-keyed and cross-week graph "
            "table across every distinct week_start in github.db, then "
            "re-run the synthesis pipeline (build_graph -> identify_units "
            "-> compute_flags) per week. Takes a SQLite backup before "
            "beginning; restores from backup on any failure. "
            "WARNING: destroys user-authored `expectation_corrections` rows "
            "(Decision 3 of epic #93 — acceptable in dev)."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: config.yaml in repo root).",
    )
    parser.add_argument(
        "--backup-dir",
        default=None,
        help=(
            "Directory to write pre-rebuild .bak files into. "
            "Default: <data_dir>/_rebuild_backup_<UTC-timestamp>/."
        ),
    )
    parser.add_argument(
        "--week",
        action="append",
        default=None,
        help=(
            "Restrict rebuild to a single week_start (repeatable). "
            "Default: every distinct week_start in graph_nodes."
        ),
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point. Returns a shell-suitable exit code (0 / 2)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)

    config = load_config(args.config)
    data_dir: Path = config.data_path
    github_db = data_dir / "github.db"
    sessions_db = data_dir / "sessions.db"
    expectations_db = data_dir / "expectations.db"
    synthesis_cfg = config.synthesis
    backup_dir = Path(args.backup_dir) if args.backup_dir else None

    try:
        summary = rebuild_history(
            github_db,
            sessions_db,
            expectations_db,
            abandonment_days=synthesis_cfg.abandonment_days,
            outlier_sigma=synthesis_cfg.outlier_sigma,
            backup_dir=backup_dir,
            weeks=args.week,
        )
    except Exception:  # noqa: BLE001 — CLI is the top of the stack
        logging.exception("am-rebuild-history failed")
        return 2

    logger.info(
        "Rebuild complete. weeks=%d corrections_destroyed=%d backup_dir=%s",
        len(summary["weeks"]),
        summary["corrections_destroyed"],
        summary["backup_dir"],
    )
    return 0


if __name__ == "__main__":  # pragma: no cover — CLI plumbing
    sys.exit(main())
