# Test Fixtures

## two_issue_session.jsonl

A session that creates two issues (#305, #312) and opens two PRs (#306, #313)
within the same session.

**Current behavior (session-hub model):** both work streams share session
`f2000000-...02` as a hub node, so connected-components produces **one unit**.

**Possible future behavior:** after the GitHub poller populates `pr_closes_issue`
edges, an aggregation layer that follows issue-direct edges instead of session
edges could produce **two units** — one per issue/PR pair.

This fixture is the reference case for that design decision (see issue #68).
