"""GitHub poller orchestrator — entry point for nightly collection.

For each configured repo:
1. Read cursor (last_polled_at)
2. Fetch issues and PRs (delta or backfill)
3. Resolve PR→issue links
4. Derive push-after-review counts
5. Upsert into github.db
6. Fetch and store edit history (body + comment edits)
7. Link PRs to sessions (via head_ref matching)
8. Advance cursor

Writes ``health.json`` only after all repos succeed.  Supports
``--dry-run`` flag (fetch and parse, skip all DB writes).

Usage:
    python -m collector.github_poller.run [--config path/to/config.yaml] [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from loguru import logger

from am_i_shipping.config_loader import GitHubLimiterConfig, load_config
from am_i_shipping.db import init_github_db
from am_i_shipping.health_writer import write_health
from am_i_shipping.logging_config import setup_logging

from .cursor import advance_cursor, compute_since, read_cursor
from .fetch_issues import (
    fetch_issue_comments,
    fetch_issue_edit_history,
    fetch_issue_edit_history_batch,
    fetch_issues,
)
from .fetch_prs import fetch_pr_comments, fetch_pr_edit_history, fetch_pr_review_comments, fetch_prs
from .gh_client import BudgetExhausted, calls_made, configure_limiter, graphql_points_used
from .link_resolver import resolve_link
from .push_counter import count_pushes_after_review
from .session_linker import link_sessions
from .store import (
    insert_issue_body_edit,
    insert_issue_comment_edit,
    insert_pr_body_edit,
    insert_pr_review_comment_edit,
    upsert_issue,
    upsert_pr,
    upsert_pr_issue_link,
)


def _apply_nice(increment: int) -> None:
    """Apply OS process deprioritization. Skips with a warning on failure."""
    try:
        os.nice(increment)
    except (OSError, AttributeError) as exc:
        logger.warning("os.nice({}) skipped: {}", increment, exc)


def _get_stored_updated_at(
    table: str,
    repo: str,
    number_col: str,
    number: int,
    db_path: Path,
    conn: Optional[sqlite3.Connection] = None,
) -> Optional[str]:
    """Return the stored ``updated_at`` value for a row, or None if not found."""
    assert table in ("issues", "pull_requests"), f"unexpected table: {table}"
    assert number_col in ("issue_number", "pr_number"), f"unexpected number_col: {number_col}"
    try:
        use_conn = conn if conn is not None else sqlite3.connect(str(db_path))
        try:
            row = use_conn.execute(
                f"SELECT updated_at FROM {table} WHERE repo = ? AND {number_col} = ?",
                (repo, number),
            ).fetchone()
            return row[0] if row else None
        finally:
            if conn is None:
                use_conn.close()
    except Exception:
        return None


def run(
    config_path: Optional[str] = None,
    dry_run: bool = False,
) -> tuple[int, bool]:
    """Run the poller for all configured repos.

    Parameters
    ----------
    config_path:
        Path to config.yaml. If None, uses the default location.
    dry_run:
        If True, fetch and parse but skip all DB writes.

    Returns
    -------
    A tuple of (total_records, all_succeeded) where *total_records* is
    the number of issues + PRs processed, and *all_succeeded* is True
    only if every configured repo was polled without error.
    """
    config = load_config(config_path)
    data_dir = config.data_path
    data_dir.mkdir(parents=True, exist_ok=True)

    github_db = data_dir / "github.db"
    sessions_db = data_dir / "sessions.db"

    logger.info(
        "polling {} repos: {} | backfill_days={} max_calls_per_hour={}",
        len(config.github.repos),
        config.github.repos,
        config.github.backfill_days,
        config.github.limiter.max_calls_per_hour,
    )

    # Apply resource limiters
    limiter = config.github.limiter
    _apply_nice(limiter.process_nice_increment)
    configure_limiter(limiter.inter_request_delay_seconds, limiter.max_calls_per_hour)

    if not dry_run:
        init_github_db(github_db)

    total_records = 0
    all_succeeded = True

    for repo in config.github.repos:
        try:
            count = _poll_repo(
                repo=repo,
                github_db=github_db,
                sessions_db=sessions_db,
                backfill_days=config.github.backfill_days,
                dry_run=dry_run,
                max_items_per_repo=limiter.max_items_per_repo,
            )
            total_records += count
            logger.info("OK: {} — {} records", repo, count)
        except BudgetExhausted as exc:
            # Stop processing further repos — the hourly budget is shared
            # across all repos and will not recover mid-run.
            logger.error("BUDGET: {} — stopping early, remaining repos skipped.", exc)
            all_succeeded = False
            break
        except Exception as exc:
            logger.error("ERROR: {} — {}", repo, exc)
            all_succeeded = False

    # Write health only if all repos succeeded and not in dry-run mode
    if all_succeeded and not dry_run:
        write_health("github_poller", total_records, data_dir=data_dir)

    return total_records, all_succeeded


def _poll_repo(
    repo: str,
    github_db: Path,
    sessions_db: Path,
    backfill_days: int,
    dry_run: bool,
    max_items_per_repo: int = 500,
) -> int:
    """Poll a single repo.  Returns the number of records processed."""
    # 1. Read cursor
    cursor_value = None if dry_run else read_cursor(repo, github_db)
    since = compute_since(cursor_value, backfill_days=backfill_days)

    logger.info("{}  mode={} since={}", repo, 'backfill' if cursor_value is None else 'delta', since)

    # 2. Fetch issues and PRs (without comments — fetched after cap to avoid
    #    wasting API calls on items that will be discarded).
    issues = fetch_issues(repo, since=since, include_comments=False)
    prs = fetch_prs(repo, since=since, include_comments=False)

    logger.info("{}  fetched {} issues, {} PRs (uncapped)", repo, len(issues), len(prs))

    if dry_run:
        return len(issues) + len(prs)

    # Apply item cap: issues first, PRs get remaining capacity.
    total_fetched = len(issues) + len(prs)
    if total_fetched > max_items_per_repo:
        if len(issues) >= max_items_per_repo:
            issues = issues[:max_items_per_repo]
            prs = []
        else:
            remaining = max_items_per_repo - len(issues)
            prs = prs[:remaining]
        logger.info(
            "{}  capped to {} issues + {} PRs (max_items_per_repo={})",
            repo, len(issues), len(prs), max_items_per_repo,
        )

    # Fetch comments only for the capped set.
    for issue in issues:
        try:
            issue["comments"] = fetch_issue_comments(repo, issue["number"])
        except BudgetExhausted as exc:
            logger.error("{}  budget exhausted fetching comments for issue #{}: {}", repo, issue["number"], exc)
            raise
        except Exception as exc:
            logger.warning("{}  comment fetch error (issue #{}): {}", repo, issue["number"], exc)
            issue["comments"] = []

    for pr in prs:
        try:
            pr["comments"] = fetch_pr_comments(repo, pr["number"])
        except BudgetExhausted as exc:
            logger.error("{}  budget exhausted fetching comments for PR #{}: {}", repo, pr["number"], exc)
            raise
        except Exception as exc:
            logger.warning("{}  comment fetch error (PR #{}): {}", repo, pr["number"], exc)
            pr["comments"] = []

        try:
            review_comments = fetch_pr_review_comments(repo, pr["number"])
            pr["review_comments"] = review_comments
            pr["review_comment_count"] = len(review_comments)
        except BudgetExhausted as exc:
            logger.error("{}  budget exhausted fetching review comments for PR #{}: {}", repo, pr["number"], exc)
            raise
        except Exception as exc:
            logger.warning("{}  review comment fetch error (PR #{}): {}", repo, pr["number"], exc)
            pr["review_comments"] = []
            pr["review_comment_count"] = 0

    # Ensure schema exists (also called by run(), but _poll_repo may be
    # invoked directly in tests), then open a single connection for all writes.
    init_github_db(github_db)
    conn = sqlite3.connect(str(github_db))
    try:
        # 3–5. Process and upsert issues
        is_backfill = cursor_value is None

        if is_backfill:
            # Backfill: upsert all issues first, then batch-fetch edit history
            for issue in issues:
                upsert_issue(repo, issue, github_db, conn=conn)

            # Batch-fetch edit history for all issues in chunks of 20
            issue_numbers = [issue["number"] for issue in issues]
            if issue_numbers:
                try:
                    batch_results = fetch_issue_edit_history_batch(repo, issue_numbers)
                except Exception as exc:
                    logger.warning("{}  edit history batch fetch error: {}", repo, exc)
                    batch_results = {}

                for issue_number, edits in batch_results.items():
                    for edit in edits.get("body_edits", []):
                        try:
                            insert_issue_body_edit(
                                repo,
                                issue_number,
                                edit["edited_at"],
                                edit.get("diff"),
                                edit.get("editor"),
                                github_db,
                                conn=conn,
                            )
                        except Exception as exc:
                            logger.warning("{}  insert issue body edit error (issue #{}): {}", repo, issue_number, exc)

                    for edit in edits.get("comment_edits", []):
                        try:
                            insert_issue_comment_edit(
                                repo,
                                issue_number,
                                edit["comment_id"],
                                edit["edited_at"],
                                edit.get("diff"),
                                edit.get("editor"),
                                github_db,
                                conn=conn,
                            )
                        except Exception as exc:
                            logger.warning("{}  insert issue comment edit error (issue #{}): {}", repo, issue_number, exc)
        else:
            # Delta: upsert each issue and fetch edit history if updated_at changed
            for issue in issues:
                prev_updated_at = _get_stored_updated_at(
                    "issues", repo, "issue_number", issue["number"], github_db,
                    conn=conn,
                )
                upsert_issue(repo, issue, github_db, conn=conn)

                current_updated_at = issue.get("updated_at")
                if current_updated_at and current_updated_at != prev_updated_at:
                    try:
                        edits = fetch_issue_edit_history(repo, issue["number"])
                    except Exception as exc:
                        logger.warning("{}  edit history fetch error (issue #{}): {}", repo, issue["number"], exc)
                        continue

                    for edit in edits.get("body_edits", []):
                        try:
                            insert_issue_body_edit(
                                repo,
                                issue["number"],
                                edit["edited_at"],
                                edit.get("diff"),
                                edit.get("editor"),
                                github_db,
                                conn=conn,
                            )
                        except Exception as exc:
                            logger.warning("{}  insert issue body edit error (issue #{}): {}", repo, issue["number"], exc)

                    for edit in edits.get("comment_edits", []):
                        try:
                            insert_issue_comment_edit(
                                repo,
                                issue["number"],
                                edit["comment_id"],
                                edit["edited_at"],
                                edit.get("diff"),
                                edit.get("editor"),
                                github_db,
                                conn=conn,
                            )
                        except Exception as exc:
                            logger.warning("{}  insert issue comment edit error (issue #{}): {}", repo, issue["number"], exc)

        # 3–5. Process PRs: resolve links, derive push counts, upsert, edit history
        for pr in prs:
            # Resolve PR→issue link
            linked_issue = resolve_link(
                head_ref=pr.get("head_ref", ""),
                body=pr.get("body", ""),
            )

            # Derive push count
            push_count = count_pushes_after_review(repo, pr["number"])
            pr["push_count"] = push_count

            if is_backfill:
                # Backfill: upsert without updated_at gating
                upsert_pr(repo, pr, github_db, conn=conn)
            else:
                # Delta: check updated_at before upsert
                prev_updated_at = _get_stored_updated_at(
                    "pull_requests", repo, "pr_number", pr["number"], github_db,
                    conn=conn,
                )
                upsert_pr(repo, pr, github_db, conn=conn)

                current_updated_at = pr.get("updated_at")
                if current_updated_at and current_updated_at != prev_updated_at:
                    try:
                        edits = fetch_pr_edit_history(repo, pr["number"])
                    except Exception as exc:
                        logger.warning("{}  edit history fetch error (PR #{}): {}", repo, pr["number"], exc)
                    else:
                        for edit in edits.get("body_edits", []):
                            try:
                                insert_pr_body_edit(
                                    repo,
                                    pr["number"],
                                    edit["edited_at"],
                                    edit.get("diff"),
                                    edit.get("editor"),
                                    github_db,
                                    conn=conn,
                                )
                            except Exception as exc:
                                logger.warning("{}  insert PR body edit error (PR #{}): {}", repo, pr["number"], exc)

                        for edit in edits.get("review_comment_edits", []):
                            try:
                                insert_pr_review_comment_edit(
                                    repo,
                                    pr["number"],
                                    edit["comment_id"],
                                    edit["edited_at"],
                                    edit.get("diff"),
                                    edit.get("editor"),
                                    github_db,
                                    conn=conn,
                                )
                            except Exception as exc:
                                logger.warning("{}  insert PR review comment edit error (PR #{}): {}", repo, pr["number"], exc)

            # Insert PR→issue link if resolved
            if linked_issue is not None:
                upsert_pr_issue_link(repo, pr["number"], linked_issue, github_db, conn=conn)

        # Backfill: fetch PR edit history in bulk after all upserts
        if is_backfill and prs:
            for pr in prs:
                try:
                    edits = fetch_pr_edit_history(repo, pr["number"])
                except Exception as exc:
                    logger.warning("{}  edit history fetch error (PR #{}): {}", repo, pr["number"], exc)
                    continue

                for edit in edits.get("body_edits", []):
                    try:
                        insert_pr_body_edit(
                            repo,
                            pr["number"],
                            edit["edited_at"],
                            edit.get("diff"),
                            edit.get("editor"),
                            github_db,
                            conn=conn,
                        )
                    except Exception as exc:
                        logger.warning("{}  insert PR body edit error (PR #{}): {}", repo, pr["number"], exc)

                for edit in edits.get("review_comment_edits", []):
                    try:
                        insert_pr_review_comment_edit(
                            repo,
                            pr["number"],
                            edit["comment_id"],
                            edit["edited_at"],
                            edit.get("diff"),
                            edit.get("editor"),
                            github_db,
                            conn=conn,
                        )
                    except Exception as exc:
                        logger.warning("{}  insert PR review comment edit error (PR #{}): {}", repo, pr["number"], exc)

        # Commit all writes for this repo
        conn.commit()
    finally:
        conn.close()

    # 6. Link PRs to sessions
    session_links = link_sessions(repo, github_db, sessions_db)
    if session_links:
        logger.info("{}  linked {} PR-session pairs", repo, session_links)

    # 7. Advance cursor
    advance_cursor(repo, github_db)
    logger.info("{}  cursor advanced to {}", repo, date.today().isoformat())

    logger.info(
        "{}  done: {} issues + {} PRs | {} REST calls, {} GraphQL points used this run",
        repo, len(issues), len(prs), calls_made(), graphql_points_used(),
    )
    # Return post-cap count (actual records written).  The uncapped fetch
    # count was already logged above; dry-run returns the uncapped count
    # earlier (before the cap is applied).
    return len(issues) + len(prs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="GitHub poller — fetch issues, PRs, and linkages"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse without writing to the database",
    )
    args = parser.parse_args()

    setup_logging()
    _total, ok = run(config_path=args.config, dry_run=args.dry_run)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
