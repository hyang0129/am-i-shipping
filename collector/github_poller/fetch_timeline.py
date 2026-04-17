"""Fetch issue timeline events for the Phase-2 synthesis engine (Epic #17, E-2).

GraphQL-only: the REST ``/issues/{n}/timeline`` endpoint is preview-API and one
round-trip per issue, which blows our hourly budget on large backfills. Instead
this module builds aliased GraphQL queries in chunks of up to 20 issues each,
mirroring the pattern already used by ``fetch_issues.fetch_issue_edit_history_batch``.

The seven event types we keep are the ones the epic ADR calls out for workflow-
unit construction (G-1/G-2): ``ASSIGNED_EVENT``, ``LABELED_EVENT``,
``UNLABELED_EVENT``, ``CLOSED_EVENT``, ``REOPENED_EVENT``,
``CROSS_REFERENCED_EVENT``, ``REFERENCED_EVENT``. Each has a slightly different
GraphQL field shape (``CrossReferencedEvent`` nests a source reference,
``LabeledEvent`` nests the label name, etc.), so we serialise whatever the
query returned as JSON into ``payload_json`` and let downstream consumers (G-1
graph builder) parse the subset they care about.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from loguru import logger

from .gh_client import BudgetExhausted, gh_graphql


# ---------------------------------------------------------------------------
# GraphQL query construction
# ---------------------------------------------------------------------------

# These are the GraphQL __typename values we keep. A timelineItems(itemTypes:)
# filter argument takes the raw GraphQL enum form, e.g. ``ASSIGNED_EVENT``.
TIMELINE_EVENT_TYPES: tuple[str, ...] = (
    "ASSIGNED_EVENT",
    "LABELED_EVENT",
    "UNLABELED_EVENT",
    "CLOSED_EVENT",
    "REOPENED_EVENT",
    "CROSS_REFERENCED_EVENT",
    "REFERENCED_EVENT",
)

# Max issues per GraphQL call — matches fetch_issue_edit_history_batch. Keeps
# individual query bodies below ~64 KB and keeps per-call GraphQL cost low.
_CHUNK_SIZE = 20

def _timeline_fragment() -> str:
    """Build the ``timelineItems`` GraphQL fragment.

    The per-event-type inline fragments pull only the fields we persist;
    actor is a viewer-typed union so we go via the shared
    ``... on User { login }`` path. Constructed per-call from
    ``TIMELINE_EVENT_TYPES`` so that runtime modifications to the tuple
    (rare, but possible from tests) are honoured.
    """
    item_types = ", ".join(TIMELINE_EVENT_TYPES)
    return f"""
      timelineItems(
        first: 100,
        itemTypes: [{item_types}]
      ) {{
        nodes {{
          __typename
          ... on AssignedEvent {{
            id
            createdAt
            actor {{ login }}
            assignee {{ ... on User {{ login }} ... on Bot {{ login }} }}
          }}
          ... on LabeledEvent {{
            id
            createdAt
            actor {{ login }}
            label {{ name }}
          }}
          ... on UnlabeledEvent {{
            id
            createdAt
            actor {{ login }}
            label {{ name }}
          }}
          ... on ClosedEvent {{
            id
            createdAt
            actor {{ login }}
            stateReason
          }}
          ... on ReopenedEvent {{
            id
            createdAt
            actor {{ login }}
          }}
          ... on CrossReferencedEvent {{
            id
            createdAt
            actor {{ login }}
            source {{
              ... on Issue {{ number repository {{ nameWithOwner }} }}
              ... on PullRequest {{ number repository {{ nameWithOwner }} }}
            }}
          }}
          ... on ReferencedEvent {{
            id
            createdAt
            actor {{ login }}
            commit {{ oid }}
            commitRepository {{ nameWithOwner }}
          }}
        }}
      }}
"""


def _build_batch_query(issue_numbers: List[int]) -> str:
    """Build an aliased GraphQL query for a chunk of issue numbers.

    The aliases are ``issue0``, ``issue1``, ... so the caller can map
    alias -> issue number by list position.
    """
    fragment = _timeline_fragment()
    alias_blocks: List[str] = []
    for i, num in enumerate(issue_numbers):
        alias_blocks.append(
            f"    issue{i}: issue(number: {num}) {{\n"
            f"      number\n"
            f"{fragment}"
            f"    }}"
        )

    return (
        "query($owner: String!, $name: String!) {\n"
        "  rateLimit { cost remaining }\n"
        "  repository(owner: $owner, name: $name) {\n"
        + "\n".join(alias_blocks)
        + "\n  }\n}"
    )


# ---------------------------------------------------------------------------
# Node normalisation
# ---------------------------------------------------------------------------

# Map GraphQL __typename back to the stable string we store in
# timeline_events.event_type. We use lowercase-with-underscore to match the
# convention the rest of the collector uses (see README for the rationale —
# downstream synthesis code pattern-matches on these strings).
_TYPENAME_TO_EVENT_TYPE: Dict[str, str] = {
    "AssignedEvent": "assigned",
    "LabeledEvent": "labeled",
    "UnlabeledEvent": "unlabeled",
    "ClosedEvent": "closed",
    "ReopenedEvent": "reopened",
    "CrossReferencedEvent": "cross-referenced",
    "ReferencedEvent": "referenced",
}


def _event_id_from_node(node: Dict[str, Any]) -> Optional[int]:
    """Extract a stable integer id from a timeline node.

    GraphQL returns ``id`` as a base64 Relay node id (string). Our table
    column is INTEGER so we hash-stabilise it: ``hash(relay_id) & 0x7FFF_FFFF``
    gives us a deterministic positive int without creating a new column.

    WARNING: collisions cause data loss, not a "safe UPDATE". A hash collision
    between two events for the same issue would have them map to the same
    ``(repo, issue_number, event_id)`` primary key, and the later UPDATE
    overwrites the earlier event's ``payload_json`` — the earlier event is
    no longer recoverable from the row because there is no row for it. The
    31-bit fold is adequate per-issue (dozens of events, collision
    probability negligible) but the birthday bound over a whole repo is not
    astronomical. If the repo-wide event count pushes into the hundreds of
    thousands, this key strategy should be revisited — either widen the
    column to TEXT and store the raw relay id, or add a separate relay id
    column with its own UNIQUE constraint.
    """
    raw = node.get("id")
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    # Python's hash() is salted per-process; we want determinism across runs
    # so the same event maps to the same row. Use a stable fold.
    h = 0
    for ch in str(raw):
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return h & 0x7FFFFFFF


def _normalize_node(issue_number: int, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Turn one GraphQL timeline node into our persistence shape.

    Returns None if the node is missing the minimal fields (no id, no type).
    Dropped nodes are logged at DEBUG so future GitHub schema drift
    (a new ``__typename`` that leaks past our ``itemTypes`` filter, or a
    null id) is observable without flooding the warning log.
    """
    typename = node.get("__typename")
    event_type = _TYPENAME_TO_EVENT_TYPE.get(typename or "")
    if event_type is None:
        logger.debug(
            "dropped timeline node for issue #{}: unknown __typename {!r}",
            issue_number, typename,
        )
        return None

    event_id = _event_id_from_node(node)
    if event_id is None:
        logger.debug(
            "dropped timeline node for issue #{}: missing id (__typename={!r})",
            issue_number, typename,
        )
        return None

    actor = (node.get("actor") or {}).get("login") if node.get("actor") else None

    return {
        "issue_number": issue_number,
        "event_id": event_id,
        "event_type": event_type,
        "actor": actor,
        "created_at": node.get("createdAt"),
        "payload_json": json.dumps(node, ensure_ascii=False, sort_keys=True),
    }


# ---------------------------------------------------------------------------
# Public fetchers
# ---------------------------------------------------------------------------

def fetch_issue_timeline_batch(
    repo: str,
    issue_numbers: List[int],
) -> Dict[int, List[Dict[str, Any]]]:
    """Fetch timeline events for a list of issues.

    Batches through :data:`_CHUNK_SIZE` issues per GraphQL call so a repo with
    hundreds of issues in the poll window costs ``ceil(n/20)`` GraphQL calls
    instead of *n* REST calls. GraphQL points are tracked by ``gh_graphql``.

    Parameters
    ----------
    repo:
        GitHub repository in ``owner/repo`` format.
    issue_numbers:
        Issue numbers to query. Empty input returns an empty dict.

    Returns
    -------
    Mapping of issue_number -> list of normalised event dicts
    (:func:`_normalize_node` shape). Issues that errored are omitted rather
    than included with an empty list, so callers can distinguish "we have
    confirmed no events" from "we never got a response".

    On per-chunk GraphQL errors the chunk is skipped with a warning log and
    processing continues — the fetcher never raises to its caller so one bad
    issue cannot take down the whole poll cycle.
    """
    if not issue_numbers:
        return {}

    owner, name = repo.split("/", 1)
    results: Dict[int, List[Dict[str, Any]]] = {}

    for chunk_start in range(0, len(issue_numbers), _CHUNK_SIZE):
        chunk = issue_numbers[chunk_start : chunk_start + _CHUNK_SIZE]
        query = _build_batch_query(chunk)

        try:
            response = gh_graphql(query, {"owner": owner, "name": name})
        except BudgetExhausted:
            # Hourly budget is repo-scoped; re-raise so the outer poll cycle
            # can stop this repo cleanly rather than logging one warning per
            # remaining chunk while the budget stays pinned.
            raise
        except Exception as exc:
            logger.warning(
                "{}  timeline batch fetch failed for chunk starting at issue #{}: {}",
                repo, chunk[0], exc,
            )
            continue

        if response.get("errors") and not response.get("data"):
            logger.warning(
                "{}  timeline GraphQL errors for chunk starting at issue #{}: {}",
                repo, chunk[0], response.get("errors"),
            )
            continue

        repo_data = (response.get("data") or {}).get("repository") or {}
        for i, num in enumerate(chunk):
            issue_data = repo_data.get(f"issue{i}")
            if not issue_data:
                # Issue may have been deleted / transferred — record an empty
                # list so the caller knows we attempted the fetch.
                results[num] = []
                continue

            nodes = (issue_data.get("timelineItems") or {}).get("nodes") or []
            events: List[Dict[str, Any]] = []
            for node in nodes:
                event = _normalize_node(num, node)
                if event is not None:
                    events.append(event)
            results[num] = events

    return results


def fetch_and_store_issue_timelines(
    repo: str,
    issue_numbers: List[int],
    github_db,
    conn=None,
) -> Dict[int, List[Dict[str, Any]]]:
    """Fetch timelines for *issue_numbers* and persist each event.

    This is the function ``_poll_repo`` calls when the
    ``github.fetch_timeline`` flag is on. Returns the same mapping as
    :func:`fetch_issue_timeline_batch` so callers can log counts.
    """
    from .store import upsert_timeline_event

    timelines = fetch_issue_timeline_batch(repo, issue_numbers)

    for issue_number, events in timelines.items():
        for event in events:
            try:
                upsert_timeline_event(repo, event, github_db, conn=conn)
            except ValueError as exc:
                logger.warning(
                    "{}  skipped timeline event for issue #{}: {}",
                    repo, issue_number, exc,
                )

    return timelines
