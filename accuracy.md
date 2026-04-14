# Accuracy

*Accuracy is a binary measure. It is 0 if the user's expectation and Claude's actual output did not match. It is 1 if they did. The principle is fixed; the unit of measurement (per session, per PR, per issue, or some other boundary) is TBD.*

---

## Definition

```
accuracy(unit) = 1  if actual output matches user expectation
               = 0  if it does not
```

There is no partial credit. A unit of work either closed the loop or it didn't.

The right unit of measurement is an open question. Per-session may be too granular — a PR typically spans multiple sessions, and the expectation is set at the issue level, not the session level. Per-PR or per-issue may be more natural. Exact measurement boundary TBD; the principle is unchanged.

---

## What Accuracy Is Not

**Reprompting is not a sufficient signal for inaccuracy.**

A user may reprompt many times within a session and still end with accuracy = 1. Reprompting is a velocity signal, not an accuracy signal. The two are independent.

---

## Examples: Accuracy = 1

**Co-coding a known-hard workflow**
The user knows Claude struggles with ffmpeg workflows. They choose to co-code — watching each step, intervening frequently, correcting course in real time. The session involves 20 exchanges. The final output matches what the user wanted. Accuracy = 1. The reprompting was not a sign of failure; it was the correct mode of working given a known capability boundary.

**Short, autonomous session**
The user states intent, Claude disambiguates, the user confirms, Claude proposes a plan, the user accepts, Claude delivers, the user reviews and approves in one pass. Three exchanges total. Accuracy = 1.

**User adjusts scope mid-session**
The user accepts a plan, Claude begins delivery, the user realizes the scope was too broad and trims it before Claude finishes. The final output matches the trimmed expectation. Accuracy = 1. The adjustment was a design-phase correction made during execution — not a failure.

---

## Examples: Accuracy = 0

**User unaware of a capability boundary**
The user is not aware that Claude has reliability issues with ffmpeg workflows. They write a clear issue, Claude autonomously executes, and the output is broken. The user expected a working ffmpeg pipeline; they got a non-working one. Accuracy = 0. The root cause is a missing precondition: the user did not know to scope the task differently or choose co-coding mode.

**Plan accepted without verification**
Claude proposes a plan. The user accepts without checking whether it follows from their confirmed motivation. Claude delivers exactly the plan — but the plan was wrong for what the user actually needed. The user reviews the output and realizes it doesn't fit. Accuracy = 0. The plan was technically executed; the expectation was not met.

**Ambiguous motivation, wrong branch taken**
The user states intent. Claude asks a disambiguation question. The user answers but the answer was itself ambiguous. Claude picks an interpretation and builds a plan. The user accepts. Claude delivers. The output reflects Claude's interpretation, not the user's actual need. Accuracy = 0. Root cause: Step 3 was not resolved before Step 4 began.

**Silent delivery drift**
Claude delivers against the accepted plan but adds scope not in the plan. The user reviews cursorily and approves. The added scope introduces a regression elsewhere. The user discovers it later and opens a follow-up issue. The original session: accuracy = 0, because the actual output did not match the expectation encoded in the accepted plan (even though the user approved it at the time).

---

## Implication for Data Collection

Accuracy cannot be read directly from session signals. Reprompt count, session duration, and turn count are velocity signals. Accuracy requires a judgment: did the session produce what the user expected?

In Phase 2, accuracy is assessed by the synthesis engine reading the session transcript and the linked issue/PR outcome — did the PR close the issue as specified, did the user request rework, did a follow-up issue appear that traces back to this session's output. It is not a field the session parser can populate.
