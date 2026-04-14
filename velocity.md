# Velocity

*Velocity is the amount of productive work generated over a period of time — where productive work means work that results in accuracy = 1.*

---

## Definition

```
velocity = volume of accuracy=1 work / time period
```

Work that resulted in accuracy = 0 does not count toward velocity. Rework, reverted PRs, and follow-up fixes caused by a bad delivery are not productive output — they are the cost of an earlier inaccuracy.

---

## Relationship to Accuracy

Accuracy is the gate. Velocity is only meaningful inside that gate. Increasing throughput at accuracy = 0 is not velocity — it is faster failure.

The goal (from `CLAUDE.md`): maintain accuracy at 95%+, then optimize velocity within that constraint. Never trade accuracy for velocity.

---

## Bug Fixes

Bug fixes count as accuracy = 1 work toward velocity.

The accuracy judgment on a bug fix applies to the PR or commit that introduced the bug, not to the fix itself. The reasoning: if a PR delivers a feature and a new bug surfaces in that new feature, the original PR is not automatically accuracy = 0. Bugs in new code are often not knowable at review time — they emerge from usage, edge cases, or interactions that weren't visible when the plan was accepted.

**The original PR is accuracy = 0 only if** the bug reflects a gap between what the user specified and what was delivered — i.e., the feature did not do what the accepted plan said it would do. That is a precondition failure (Step 4 or Step 7). A bug that emerges later, in a behavior the plan did not address, is not retroactive inaccuracy.

### Examples

**Bug fix, original PR = accuracy 1**
A PR adds a new file export feature. The plan specified the export format and the happy path. After merging, a user reports that the export silently truncates files over 2GB. The 2GB edge case was not in the plan and was not knowable from the issue. The original PR: accuracy = 1. The bug fix: accuracy = 1. Both count toward velocity.

**Bug fix, original PR = accuracy 0**
A PR is supposed to add input validation to a form. The accepted plan explicitly lists which fields must be validated. After merging, one of the listed fields has no validation. The user discovers it. The original PR did not deliver what the plan said. Accuracy = 0. The bug fix restores what should have been there. The bug fix counts toward velocity; the original PR does not.

---

## Perspectives on Velocity

Velocity can be measured from multiple angles. All are worth tracking; none alone is sufficient.

- **Lines of code** — coarse but objective. Confounded by verbosity and refactors.
- **Issues closed / PRs merged per week** — cleaner unit, directly tied to delivery. Preferred for trend tracking.
- **Leverage** — the preferred lens in this project (see below).

---

## Leverage

Leverage is the amount of human time converted into productive coding time (accuracy = 1 output).

```
leverage = productive coding output / human time invested
```

A human hour that produces 10 hours of accurate Claude coding output has 10x leverage. A human hour of co-coding that produces 1 hour of accurate output has 1x leverage.

Non-coding workflows — render pipelines, data exports — do not count toward leverage or velocity. The scope is coding work. Deployment work counts (it is code: scripts, config, infrastructure).

### The Leverage Spectrum

Leverage is determined by how much of the design phase (Steps 1–5) the user completes before execution begins. The more front-loaded the preparation, the less human time is required during Step 6.

```
Low leverage                                    High leverage
     │                                               │
     ▼                                               ▼
Co-coding          Hook mode          Autonomous agent
(user present      (user present      (user absent
 throughout)        at handoff only)   entirely)
```

**Co-coding** is the lowest leverage mode (above manual coding). The user is present throughout Step 6 — steering, correcting, approving each increment. Human time and Claude time are roughly 1:1.

**Higher autonomy** is achieved through better preparation. The more precisely Steps 1–5 are executed — motivation confirmed, plan specific and bounded, environment correct — the less the user needs to intervene during Step 6. A well-prepared autonomous session converts one hour of design-phase work into many hours of accurate execution with zero human presence during delivery.

**The leverage ceiling is set in the design phase.** You cannot increase leverage by optimizing Step 6. Leverage is granted through better preparation, not through Claude becoming more capable mid-session.

### Implications

- A co-coding session with accuracy = 1 is not a failure — it may be the correct mode for the task. But it is a low-leverage outcome and should prompt the question: could better preparation (CLAUDE.md, a more specific plan, clearer motivation) have enabled autonomy instead?
- Reprompting during a session is a leverage cost, not just a velocity cost. Each reprompt is human time consumed that better preparation could have avoided.
- The experiment loop (Phase 3) targets leverage: identify which design-phase step, when done better, converts co-coding sessions into autonomous ones.

---

## What Velocity Is Not

**Reprompt count is not an inverse velocity signal on its own.** A high-reprompt session that ends in accuracy = 1 is productive — it just has a leverage cost. The reprompting is a signal to investigate, not a verdict.

**Session count is not velocity.** More sessions do not mean more output. Sessions are inputs; accuracy = 1 PRs or issues closed are outputs.

---

## Measurement

Like accuracy, the exact unit and measurement boundary for velocity is TBD. Candidates: PRs merged per week, issues closed per week — filtered to those with accuracy = 1. For leverage specifically: ratio of human-present time (co-coding sessions, reprompt exchanges) to total accurate output, tracked over time. The principle is stable regardless of which unit is chosen.
