# Weekly Retrospective

## Velocity Trend

No historical baseline available yet; this is the first synthesised
week. Record this week's per-unit elapsed_days and total_reprompts as
the anchor for next week's delta.

## Unit Summary Table

| unit_id | root_node | elapsed_days | dark_time_pct | total_reprompts | review_cycles | status |
|---------|-----------|--------------|---------------|-----------------|---------------|--------|
| (populated from units table for the given week_start) | | | | | | |

## Outlier Units

Outlier units this week are those whose metric value exceeds
``median + 2sigma`` across the week's population. Empty list means no
unit breached the threshold — not that the pass did not run.

## Abandoned Units

Units with no graph_nodes event within the last 14 days and no open
issue/PR are flagged as abandoned. The abandonment flag is a hint for
triage, not a prescription.

## Dark Time

Dark time is the fraction of the unit's wall-clock span during which
no session was active. Single-session units report 0.0 by definition
(see ADR Decision 3).

## Clarifying Questions

1. Of the flagged outlier units, which one surprises you most, and what
   precondition do you think was missing when the work started?
2. For the abandoned units, is the plan still valid but deprioritised,
   or did the motivation evaporate once scoping revealed the true cost?
