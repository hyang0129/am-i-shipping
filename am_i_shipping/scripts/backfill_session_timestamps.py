"""Backfill ``session_started_at`` / ``session_ended_at`` for historical rows.

Epic #17 — Sub-Issue 2/7 (#35). Sub-Issue 1 landed the two new columns in
``sessions`` with NULL defaults for every existing row. New inserts populated
by ``collector.session_parser`` write the timestamps directly, but the
15,505+ pre-existing rows need a one-shot backfill. This script walks those
rows, re-opens the JSONL session file that produced each one, and UPDATEs
only the two timestamp columns in place — every other column (especially
``raw_content_json``) is left alone.

``raw_content_json`` intentionally strips per-turn timestamps by design (see
``project_am_i_shipping_design``), so the database column cannot be
recovered from the SQL row alone. The JSONL file is the single source of
truth. Rows whose JSONL file is no longer on disk are left with NULL
timestamps and logged as a warning — a clean exit is preferable to partial
updates that mask missing data.

Invocation::

    python -m am_i_shipping.scripts.backfill_session_timestamps \
        [--config path/to/config.yaml] [--dry-run] [--limit N]

``--dry-run`` skips the UPDATE and prints the counts it would have written.
``--limit`` caps the number of rows processed per invocation (useful for
smoke-testing on production DBs before a full run).

This is NOT wired into the collector loop; it is run manually exactly once
per environment. Running it a second time is harmless — rows whose
timestamps are already populated are skipped by the ``WHERE`` clause.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from loguru import logger


def _find_session_file(
    projects_path: Path,
    session_uuid: str,
    index: Dict[str, Path],
) -> Optional[Path]:
    """Return the JSONL file containing *session_uuid*, if one exists.

    Uses a pre-built ``index`` mapping session_uuid -> path. The caller
    builds the index once per invocation by scanning *projects_path* — this
    makes the backfill O(total_files + rows_to_update) instead of O(
    total_files * rows_to_update).
    """
    return index.get(session_uuid)


def _build_session_index(projects_path: Path) -> Dict[str, Path]:
    """Scan *projects_path* and build a session_uuid -> file path map.

    Matches the discovery convention used by ``collector.session_parser``:
    every ``*.jsonl`` under *projects_path* except those under a
    ``subagents/`` subdirectory.

    Only the first ``sessionId`` found in each file is indexed — sessions
    never span multiple files in the JSONL format, so this is exact.
    """
    if not projects_path.exists():
        return {}

    index: Dict[str, Path] = {}
    for jsonl in projects_path.rglob("*.jsonl"):
        if "subagents" in jsonl.parts:
            continue

        try:
            with open(jsonl, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    sid = entry.get("sessionId")
                    if sid:
                        # First writer wins; subsequent references (including
                        # the same session UUID appearing twice in the same
                        # file) are ignored.
                        index.setdefault(sid, jsonl)
                        break
        except OSError:
            # Unreadable file — skip and continue; the row stays NULL.
            continue

    return index


def _extract_timestamps(jsonl_path: Path) -> Optional[Tuple[str, str]]:
    """Return ``(first_ts_iso, last_ts_iso)`` from a JSONL session file.

    Returns ``None`` if the file has zero parseable timestamps (empty session
    or malformed). The timestamps are taken across **all** entry types, not
    just user/assistant turns, so the bounds include the initial
    ``queue-operation`` markers as well — this matches what
    ``session_parser.parse_session`` now stores.
    """
    timestamps = []
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts_str = entry.get("timestamp")
                if not ts_str:
                    continue
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except ValueError:
                    continue
                timestamps.append(ts)
    except OSError:
        return None

    if not timestamps:
        return None

    # Preserve the source ordering — JSONL session files are written
    # append-only so index 0 is first and -1 is last. min/max would be
    # equivalent on a well-formed file; using the list ends also matches the
    # behaviour of session_parser.parse_session.
    return timestamps[0].isoformat(), timestamps[-1].isoformat()


def backfill(
    sessions_db: Path,
    projects_path: Path,
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> Tuple[int, int, int]:
    """Run the backfill.

    Returns ``(updated, skipped_no_file, errored)``:

    * ``updated`` — rows whose timestamps were (or would be, in dry-run mode)
      written.
    * ``skipped_no_file`` — rows whose JSONL file could not be found under
      *projects_path* (session log rotated out or machine moved).
    * ``errored`` — rows where the JSONL was located but parsing failed.
    """
    if not sessions_db.exists():
        raise FileNotFoundError(f"sessions.db not found: {sessions_db}")

    logger.info("building session_uuid index under {} ...", projects_path)
    index = _build_session_index(projects_path)
    logger.info("indexed {} session files", len(index))

    conn = sqlite3.connect(str(sessions_db))
    try:
        query = (
            "SELECT session_uuid FROM sessions "
            "WHERE session_started_at IS NULL OR session_ended_at IS NULL"
        )
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        cursor = conn.execute(query)
        targets = [row[0] for row in cursor.fetchall()]
        logger.info("{} rows need backfill (limit={})", len(targets), limit)

        updated = 0
        skipped_no_file = 0
        errored = 0

        for session_uuid in targets:
            jsonl = _find_session_file(projects_path, session_uuid, index)
            if jsonl is None:
                skipped_no_file += 1
                logger.debug("no JSONL file for session {}", session_uuid)
                continue

            stamps = _extract_timestamps(jsonl)
            if stamps is None:
                errored += 1
                logger.warning(
                    "could not extract timestamps from {} for session {}",
                    jsonl, session_uuid,
                )
                continue

            first_ts, last_ts = stamps

            if dry_run:
                updated += 1
                continue

            # UPDATE — and ONLY these two columns. A narrow SET list is the
            # whole point of this script: every other column (raw_content_json,
            # token counts, working_directory, ...) is preserved byte-for-byte.
            conn.execute(
                "UPDATE sessions "
                "SET session_started_at = ?, session_ended_at = ? "
                "WHERE session_uuid = ?",
                (first_ts, last_ts, session_uuid),
            )
            updated += 1

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return updated, skipped_no_file, errored


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill session_started_at / session_ended_at columns for "
            "rows inserted before Epic #17 Sub-Issue 1 landed the schema."
        )
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to config.yaml (default: config.yaml in repo root)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print counts without UPDATEing any rows.",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Cap number of rows processed (for smoke-testing).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Deferred import so ``--help`` works even with a broken config.yaml.
    from am_i_shipping.config_loader import load_config

    config = load_config(args.config)
    sessions_db = config.data_path / "sessions.db"
    projects_path = Path(config.session.projects_path)

    updated, skipped, errored = backfill(
        sessions_db, projects_path,
        dry_run=args.dry_run,
        limit=args.limit,
    )

    mode = "DRY-RUN — " if args.dry_run else ""
    print(
        f"{mode}backfill complete: {updated} updated, "
        f"{skipped} skipped (no JSONL), {errored} errored",
        file=sys.stderr,
    )
    return 0 if errored == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
