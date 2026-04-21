"""GitHub poller orchestrator — entry point for nightly collection.

For each configured repo:
1. Read cursor (last_polled_at)
2. Fetch issues and PRs (delta or backfill)
3. Resolve PR→issue links
4. Derive push-after-review counts
5. Upsert into github.db
6. Fetch and store edit history (body + comment edits)
7. Fetch and store per-PR commits (Epic #17 E-1, config-flagged)
8. Fetch and store issue timeline events (Epic #17 E-2, config-flagged)
9. Link PRs to sessions (via head_ref matching)
10. Advance cursor

Writes ``health.json`` only after all repos succeed.  Supports
``--dry-run`` flag (fetch and parse, skip all DB writes).

The per-repo orchestration is split into named helpers
(``_apply_item_cap``, ``_attach_comments``, ``_process_issues``,
``_process_prs``, ``_fetch_timeline_step``) so new collector stages slot in
cleanly. The outer ``_poll_repo`` is a sequence-of-steps composition;
behavior of the pre-existing steps is unchanged from the monolithic version.

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
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from am_i_shipping.config_loader import GitHubConfig, load_config
from am_i_shipping.db import init_github_db
from am_i_shipping.health_writer import write_health
from am_i_shipping.logging_config import setup_logging

from .cursor import advance_cursor, compute_since, read_cursor
from .fetch_commits import fetch_and_store_pr_commits
from .fetch_issues import (
    fetch_issue_comments,
    fetch_issue_edit_history,
    fetch_issue_edit_history_batch,
    fetch_issues,
)
from .fetch_prs import fetch_pr_comments, fetch_pr_edit_history, fetch_pr_review_comments, fetch_prs
from .fetch_timeline import fetch_and_store_issue_timelines
from .gh_client import BudgetExhausted, GhCliError, calls_made, configure_limiter, graphql_points_used
from .link_resolver import resolve_link
from .push_counter import count_pushes_after_review
from .issue_linker import link_issues
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
        "polling {} repos: {} | backfill_days={} max_calls_per_hour={} "
        "fetch_commits={} fetch_timeline={}",
        len(config.github.repos),
        config.github.repos,
        config.github.backfill_days,
        config.github.limiter.max_calls_per_hour,
        config.github.fetch_commits,
        config.github.fetch_timeline,
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
                fetch_commits_enabled=config.github.fetch_commits,
                fetch_timeline_enabled=config.github.fetch_timeline,
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


# ---------------------------------------------------------------------------
# Per-repo pipeline — split into named helpers so new stages slot in cleanly.
# ---------------------------------------------------------------------------


def _apply_item_cap(
    repo: str,
    issues: List[Dict[str, Any]],
    prs: List[Dict[str, Any]],
    max_items_per_repo: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Cap the combined issue+PR count to *max_items_per_repo*.

    Issues take priority — PRs consume whatever capacity remains. Returns the
    two capped lists. Logs only when a cap is actually applied.
    """
    total_fetched = len(issues) + len(prs)
    if total_fetched <= max_items_per_repo:
        return issues, prs

    if len(issues) >= max_items_per_repo:
        capped_issues = issues[:max_items_per_repo]
        capped_prs: List[Dict[str, Any]] = []
    else:
        remaining = max_items_per_repo - len(issues)
        capped_issues = issues
        capped_prs = prs[:remaining]

    logger.info(
        "{}  capped to {} issues + {} PRs (max_items_per_repo={})",
        repo, len(capped_issues), len(capped_prs), max_items_per_repo,
    )
    return capped_issues, capped_prs


def _attach_comments(
    repo: str,
    issues: List[Dict[str, Any]],
    prs: List[Dict[str, Any]],
) -> None:
    """Fetch and attach comments and review comments in place.

    Budget-exhausted errors propagate up (the poll cycle for this repo aborts);
    all other errors are logged and the offending item gets empty comments.
    """
    for issue in issues:
        try:
            issue["comments"] = fetch_issue_comments(repo, issue["number"])
        except BudgetExhausted as exc:
            logger.error(
                "{}  budget exhausted fetching comments for issue #{}: {}",
                repo, issue["number"], exc,
            )
            raise
        except Exception as exc:
            logger.warning(
                "{}  comment fetch error (issue #{}): {}",
                repo, issue["number"], exc,
            )
            issue["comments"] = []

    for pr in prs:
        try:
            pr["comments"] = fetch_pr_comments(repo, pr["number"])
        except BudgetExhausted as exc:
            logger.error(
                "{}  budget exhausted fetching comments for PR #{}: {}",
                repo, pr["number"], exc,
            )
            raise
        except Exception as exc:
            logger.warning(
                "{}  comment fetch error (PR #{}): {}",
                repo, pr["number"], exc,
            )
            pr["comments"] = []

        try:
            review_comments = fetch_pr_review_comments(repo, pr["number"])
            pr["review_comments"] = review_comments
            pr["review_comment_count"] = len(review_comments)
        except BudgetExhausted as exc:
            logger.error(
                "{}  budget exhausted fetching review comments for PR #{}: {}",
                repo, pr["number"], exc,
            )
            raise
        except Exception as exc:
            logger.warning(
                "{}  review comment fetch error (PR #{}): {}",
                repo, pr["number"], exc,
            )
            pr["review_comments"] = []
            pr["review_comment_count"] = 0


def _process_issues(
    repo: str,
    issues: List[Dict[str, Any]],
    github_db: Path,
    conn: sqlite3.Connection,
    is_backfill: bool,
) -> None:
    """Upsert issues and fetch + persist edit history.

    In backfill mode we do a single batched GraphQL call for edit history;
    in delta mode we fetch per-issue only when ``updated_at`` has changed.
    """
    if is_backfill:
        for issue in issues:
            upsert_issue(repo, issue, github_db, conn=conn)

        issue_numbers = [issue["number"] for issue in issues]
        if not issue_numbers:
            return

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
                    logger.warning(
                        "{}  insert issue body edit error (issue #{}): {}",
                        repo, issue_number, exc,
                    )

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
                    logger.warning(
                        "{}  insert issue comment edit error (issue #{}): {}",
                        repo, issue_number, exc,
                    )
        return

    # Delta: one at a time; only re-fetch edits when updated_at changed.
    for issue in issues:
        prev_updated_at = _get_stored_updated_at(
            "issues", repo, "issue_number", issue["number"], github_db,
            conn=conn,
        )
        upsert_issue(repo, issue, github_db, conn=conn)

        current_updated_at = issue.get("updated_at")
        if not current_updated_at or current_updated_at == prev_updated_at:
            continue

        try:
            edits = fetch_issue_edit_history(repo, issue["number"])
        except Exception as exc:
            logger.warning(
                "{}  edit history fetch error (issue #{}): {}",
                repo, issue["number"], exc,
            )
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
                logger.warning(
                    "{}  insert issue body edit error (issue #{}): {}",
                    repo, issue["number"], exc,
                )

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
                logger.warning(
                    "{}  insert issue comment edit error (issue #{}): {}",
                    repo, issue["number"], exc,
                )


def _process_prs(
    repo: str,
    prs: List[Dict[str, Any]],
    github_db: Path,
    conn: sqlite3.Connection,
    is_backfill: bool,
    *,
    fetch_commits_enabled: bool,
) -> None:
    """Resolve links, derive push counts, upsert PRs, handle edit history.

    When ``fetch_commits_enabled`` is True the per-PR ``/commits`` round-trip
    is made once (via ``fetch_commits.fetch_and_store_pr_commits``) and the
    commit list is passed to ``count_pushes_after_review`` so it does not
    repeat the call.
    """
    for pr in prs:
        # Resolve PR→issue link
        linked_issue = resolve_link(
            head_ref=pr.get("head_ref", ""),
            body=pr.get("body", ""),
        )

        # Epic #17 E-1: fetch+persist commits once, reuse for push_count.
        # This is the *only* place in the pipeline that hits /commits per PR.
        # If fetch_commits raises (transient GhCliError), we fall back to the
        # pre-E1 path of letting push_counter do its own /commits fetch — that
        # preserves push_count fidelity rather than collapsing to 0 on a flaky
        # API call (see F-2). BudgetExhausted is not caught so it propagates.
        pre_fetched_commits: Optional[List[Dict[str, Any]]] = None
        if fetch_commits_enabled:
            try:
                pre_fetched_commits = fetch_and_store_pr_commits(
                    repo, pr["number"], github_db, conn=conn,
                )
            except GhCliError as exc:
                logger.warning(
                    "{}  fetch_commits failed (PR #{}): {} — falling back to "
                    "push_counter self-fetch",
                    repo, pr["number"], exc,
                )
                pre_fetched_commits = None

        # Derive push count — reuse commits if we already pulled them, else
        # fall back to push_counter's own /commits fetch (pre-E1 behaviour).
        # push_count is consumed by upsert_pr below via pr["push_count"].
        if pre_fetched_commits is not None:
            push_count = count_pushes_after_review(
                repo, pr["number"], commits=pre_fetched_commits,
            )
        else:
            push_count = count_pushes_after_review(repo, pr["number"])
        pr["push_count"] = push_count

        if is_backfill:
            upsert_pr(repo, pr, github_db, conn=conn)
        else:
            # Delta: check updated_at before upsert so we only pull edits on
            # genuinely changed rows.
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
                    logger.warning(
                        "{}  edit history fetch error (PR #{}): {}",
                        repo, pr["number"], exc,
                    )
                else:
                    _persist_pr_edits(repo, pr["number"], edits, github_db, conn)

        # Insert PR→issue link if resolved
        if linked_issue is not None:
            upsert_pr_issue_link(repo, pr["number"], linked_issue, github_db, conn=conn)

    # Backfill: fetch PR edit history in bulk after all upserts
    if is_backfill and prs:
        for pr in prs:
            try:
                edits = fetch_pr_edit_history(repo, pr["number"])
            except Exception as exc:
                logger.warning(
                    "{}  edit history fetch error (PR #{}): {}",
                    repo, pr["number"], exc,
                )
                continue
            _persist_pr_edits(repo, pr["number"], edits, github_db, conn)


def _persist_pr_edits(
    repo: str,
    pr_number: int,
    edits: Dict[str, Any],
    github_db: Path,
    conn: sqlite3.Connection,
) -> None:
    """Insert PR body + review comment edits. Split out so both the delta and
    backfill branches of ``_process_prs`` share one body."""
    for edit in edits.get("body_edits", []):
        try:
            insert_pr_body_edit(
                repo,
                pr_number,
                edit["edited_at"],
                edit.get("diff"),
                edit.get("editor"),
                github_db,
                conn=conn,
            )
        except Exception as exc:
            logger.warning(
                "{}  insert PR body edit error (PR #{}): {}",
                repo, pr_number, exc,
            )

    for edit in edits.get("review_comment_edits", []):
        try:
            insert_pr_review_comment_edit(
                repo,
                pr_number,
                edit["comment_id"],
                edit["edited_at"],
                edit.get("diff"),
                edit.get("editor"),
                github_db,
                conn=conn,
            )
        except Exception as exc:
            logger.warning(
                "{}  insert PR review comment edit error (PR #{}): {}",
                repo, pr_number, exc,
            )


def _fetch_timeline_step(
    repo: str,
    issues: List[Dict[str, Any]],
    github_db: Path,
    conn: sqlite3.Connection,
) -> None:
    """Epic #17 E-2: fetch + persist timeline events for the polled issues."""
    issue_numbers = [issue["number"] for issue in issues]
    if not issue_numbers:
        return

    try:
        timelines = fetch_and_store_issue_timelines(
            repo, issue_numbers, github_db, conn=conn,
        )
    except BudgetExhausted:
        # Propagate so the outer run() stops this repo cleanly.
        raise
    except Exception as exc:
        logger.warning("{}  timeline fetch error: {}", repo, exc)
        return

    total_events = sum(len(v) for v in timelines.values())
    if total_events:
        logger.info(
            "{}  persisted {} timeline events across {} issues",
            repo, total_events, len(timelines),
        )


# ---------------------------------------------------------------------------
# _poll_repo — composition of the helpers above.
# ---------------------------------------------------------------------------

def _poll_repo(
    repo: str,
    github_db: Path,
    sessions_db: Path,
    backfill_days: int,
    dry_run: bool,
    max_items_per_repo: int = 500,
    *,
    fetch_commits_enabled: bool = True,
    fetch_timeline_enabled: bool = True,
) -> int:
    """Poll a single repo.  Returns the number of records processed.

    The new ``fetch_commits_enabled`` and ``fetch_timeline_enabled`` kwargs
    (Epic #17 Sub-Issue 2) gate the new E-1 and E-2 collectors. When either
    flag is False the corresponding step is skipped entirely — the rest of
    the poll cycle (issues, PRs, push counter, edit history, session linking)
    runs exactly as it did before this sub-issue.
    """
    # 1. Read cursor
    cursor_value = None if dry_run else read_cursor(repo, github_db)
    since = compute_since(cursor_value, backfill_days=backfill_days)

    logger.info(
        "{}  mode={} since={}",
        repo, 'backfill' if cursor_value is None else 'delta', since,
    )

    # 2. Fetch issues and PRs (without comments — fetched after cap to avoid
    #    wasting API calls on items that will be discarded).
    issues = fetch_issues(repo, since=since, include_comments=False)
    prs = fetch_prs(repo, since=since, include_comments=False)

    logger.info("{}  fetched {} issues, {} PRs (uncapped)", repo, len(issues), len(prs))

    if dry_run:
        return len(issues) + len(prs)

    # 3. Apply item cap.
    issues, prs = _apply_item_cap(repo, issues, prs, max_items_per_repo)

    # 4. Fetch comments only for the capped set.
    _attach_comments(repo, issues, prs)

    # 5. Ensure schema exists (also called by run(), but _poll_repo may be
    # invoked directly in tests), then open a single connection for all writes.
    init_github_db(github_db)
    conn = sqlite3.connect(str(github_db))
    try:
        is_backfill = cursor_value is None

        # 6. Process issues (upsert + edit history).
        _process_issues(repo, issues, github_db, conn, is_backfill)

        # 7. Process PRs (link resolution, push counts, upsert, edit history,
        # and — when enabled — E-1 commit persistence inline so push_counter
        # can reuse the commit list).
        _process_prs(
            repo, prs, github_db, conn, is_backfill,
            fetch_commits_enabled=fetch_commits_enabled,
        )

        # 8. E-2 timeline events.
        if fetch_timeline_enabled:
            _fetch_timeline_step(repo, issues, github_db, conn)

        # Commit all writes for this repo
        conn.commit()
    finally:
        conn.close()

    # 10. Link PRs to sessions
    session_links = link_sessions(repo, github_db, sessions_db)
    if session_links:
        logger.info("{}  linked {} PR-session pairs", repo, session_links)

    # 10b. Link issues to sessions
    issue_links = link_issues(repo, github_db, sessions_db)
    if issue_links:
        logger.info("{}  linked {} issue-session pairs", repo, issue_links)

    # 11. Advance cursor
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
