# Idealized Workflow

*This document defines the target collaboration pattern between user and Claude Code. All data collection and synthesis in this project exist to measure adherence to this workflow, detect deviations early, and steer back toward it.*

---

## Scale of Automation

This workflow is not specific to any one way of working with Claude. The seven steps apply equally in two modes:

**Issue → PR (high automation)**
The user works at the level of issues and pull requests. Steps 1–5 happen in the issue thread and planning conversation. Step 6 is Claude running autonomously — often across multiple agents, subagents, and sessions. Step 7 is a PR review. The user is mostly absent during Step 6.

**Co-coding (low automation)**
The user is present throughout. Steps 1–5 may compress into a few turns of a single session. Step 6 happens incrementally with the user watching. Step 7 is in-session review of each output before moving on.

The steps are the same. What changes is the **granularity** at which they occur and the **degree of user presence** during Step 6. In the issue → PR mode, precondition failures at Steps 1–5 have a much higher cost because Step 6 runs unattended — a bad plan executes fully before the user sees the result. In co-coding mode, the user can intervene mid-delivery but risks skipping the design steps entirely because everything feels fast and informal.

Both modes are valid. Both are subject to the same failure modes. The preconditions to Step 6 matter in both.

**Constraint — QA tasks:** Some quality assurance tasks cannot yet be reliably executed by Claude. UI verification, end-to-end testing across real environments, and judgment calls that require visual or experiential validation are not fully automatable at the current capability level. In these cases Step 6 is intentionally bounded — Claude delivers the implementation, and the user owns QA. This is not a workflow failure; it is a correct scoping of what Step 6 covers. Plans (Step 4) should explicitly mark QA steps that fall outside Claude's reliable execution boundary so the user knows what they are taking on before accepting.

---

## Root-Cause Philosophy

Deviations almost always originate earlier than they appear. A failure visible in the execution phase (steps 6–7) is typically rooted in the design phase (steps 1–5). A failure in the design phase is often rooted in Phase 0. **Look for where the problem started, not where it surfaced.**

```
Setup Phase  →  Design Phase  →  Execution Phase
              (steps 1–5)       (steps 6–7)

Cause                              Effect
originates ───────────────────► surfaces here
here
```

Symptoms are downstream. Causes are upstream. Fixing the symptom without finding the root is noise.

**All deviations are user errors.** Claude's behavior is not under our control. The workflow, the setup, the prompts, the environment, the review habits — those are. Every deviation is a signal about something the user can change.

---

## The Phases

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SETUP PHASE
  Precondition for everything else.
  Failures here corrupt all downstream phases.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

0. Environment is correctly configured

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DESIGN PHASE  (steps 1–5)
  Time here is investment, not waste.
  Extended back-and-forth is expected and healthy.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. User states intent
       ↓
2. Claude disambiguates motivation
       ↓
3. Motivation confirmed
       ↓
4. Claude proposes plan
       ↓
5. Plan confirmed

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  EXECUTION PHASE  (steps 6–7)
  This is where failures are expensive.
  Rework here means an earlier phase failed.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

6. Claude delivers
       ↓
7. User reviews and approves
```

---

### Setup Phase

The Setup Phase is the most important part of the workflow. Everything else depends on it. A well-executed Setup Phase is what allows Claude to confirm motivation, propose correct plans, and deliver without constant user intervention. A poor Setup Phase forces the user back into the loop at every step — supplying context verbally, correcting misaligned plans, catching errors that should have been impossible.

The Setup Phase is not a step the user takes during a session. It is an ongoing investment in the conditions under which Claude operates. Its failures are silent: Claude proceeds without the context it needs, results degrade, and the diagnosis happens sessions later (if at all).

#### What the Setup Phase covers

**Goals and intent model.** The most important thing to document is *what the project is trying to achieve and why*. Not just what the code does — what problem it solves, what good looks like, what trade-offs have already been made. When Claude understands the goals deeply, it can resolve ambiguous intents without asking. "Make this better" becomes actionable when Claude knows what better means in this repo.

**Ambiguity resolution rules.** Every project has recurring forks — places where a request could reasonably go in two directions. Document how to resolve them. Which direction does this repo take when performance and readability conflict? When a fix could be minimal or structural, which is preferred? When a feature could be scoped narrowly or broadly, what's the default? These rules are what allow Claude to confirm motivation at Step 3 without asking.

**Coding practices and patterns.** Document how the codebase is structured, what conventions are in use, and what patterns are preferred. This is what allows Claude to produce a correct plan at Step 4 without discretionary choices — the conventions resolve the plan.

**Known weaknesses and recurring issues.** Document the areas of the codebase that have caused problems before, the failure modes that have surfaced, and the constraints that are easy to violate. This allows Claude to avoid them proactively rather than discovering them during delivery.

**Environment and tooling.** CLAUDE.md files present and current, Claude Code hooks registered, the relevant repo's venv active, `gh` CLI authenticated, and any project-specific tooling in place. These are the mechanical preconditions — necessary but not sufficient.

#### The leverage point

The Setup Phase is where the user's investment has the highest return. One hour spent documenting goals and ambiguity resolution rules can eliminate user confirmation at Steps 3 and 5 across every future session. The alternative is paying that cost turn by turn, indefinitely.

**Healthy signal:** Claude reads the correct CLAUDE.md at session start. Steps 3 and 5 resolve without user input. Tool calls succeed on first attempt. No time spent re-explaining context that should have been in config.

**Deviation — Goals not documented:** CLAUDE.md describes the codebase mechanically but not what the project is trying to achieve. Claude cannot resolve motivational ambiguity and falls back to asking. Fix: add a goals section that explains what the project is for and what good outcomes look like.

**Deviation — Ambiguity resolution rules missing:** Recurring forks are not documented. Claude asks the same clarifying questions across multiple sessions. Fix: each time the user has to answer a clarifying question, ask whether the answer belongs in CLAUDE.md so it doesn't need to be asked again.

**Deviation — CLAUDE.md missing or stale:** The user did not create or maintain the project's context file. Claude operates without project-specific constraints and produces generic plans. Fix: update CLAUDE.md before starting work in a repo. Treat it as a living document — every session where context had to be supplied verbally is a signal that something is missing.

**Deviation — Hook not registered:** The user did not register the Claude Code session hook. Session data is not captured. Fix: register the hook as documented in `setup.md`.

**Deviation — Wrong environment active:** The user started a session without setting up the correct venv or credentials. Tool call failures at Step 6 inherit this debt. Fix: establish a session start checklist.

---

### Step 1 — User States Intent

The user expresses what they want. The statement may be vague, high-level, or precise — that's fine. The intent is a starting point, not a contract.

**Note:** Time spent in the design phase (steps 1–5) is not a friction signal. Extended back-and-forth here is not a problem. What matters is that the steps are completed in order, not how long they take.

**Deviation — Repeated restatement of the same intent:** The user did not provide enough signal in the initial prompt for Claude to engage with. Rather than adding specificity, they repeated the same words. Fix: when Claude's response misses the mark, add context about why — not the same request again.

---

### Step 2 — Claude Disambiguates Motivation

Before proposing anything, Claude investigates the *why* behind the intent. There is almost always more than one valid reason a user might want X. Claude surfaces the most likely alternatives and asks the user to confirm which one applies.

Format: *"Do you want X because A — or X because B?"*

**Healthy signal:** Claude asks one question that correctly identifies the real fork. The user can answer in one sentence.

**Deviation — User pushed past disambiguation:** The user did not wait for or encourage Claude to ask why — they provided so much detail in Step 1 that Claude skipped to a plan, or the user explicitly said "just do it." The resulting plan may be right by coincidence. Fix: leave room for Claude to ask. Resist the urge to over-specify upfront.

**Deviation — User accepted a wrong disambiguation framing:** Claude asked about the wrong fork and the user answered it anyway rather than correcting the question. The plan that follows will be grounded in a false premise. Fix: if Claude's clarifying question doesn't feel right, say so before answering it.

**Deviation — User answered with new ambiguity:** The response to Claude's question opened more questions instead of closing the fork. The user had not clarified their own motivation before the session started. Fix: before starting a session, know why you want the thing, not just what.

---

### Step 3 — Motivation Confirmed

The motivation is confirmed — either by Claude selecting it from context, or by the user when Claude cannot. This confirmation is the anchor for everything that follows.

**Claude confirms when:** the Setup Phase is sufficient for Claude to identify the correct motivation behind the intent. With complete setup, this should cover almost any intent that isn't completely vague — even something like "make this better" should be resolvable if the repo's goals, quality standards, and known weaknesses are correctly defined in the Setup Phase. Claude states the inferred motivation and proceeds.

**User confirmation is required when:** the Setup Phase is insufficient for Claude to select the correct motivation with confidence. User confirmation is not a normal step — it is a signal that the Setup Phase is incomplete. The more often user confirmation is needed, the more the Setup Phase needs work.

**Healthy signal:** Claude states the motivation and moves to Step 4 without asking. User confirmation should be rare.

**Deviation — Confirmation is "both" or "neither":** The user had not resolved the ambiguity in their own thinking before the session began. Fix: stop the session, clarify the goal offline, restart with a cleaner prompt.

---

### Step 4 — Claude Proposes Plan

With confirmed motivation in hand, Claude proposes a specific, bounded plan. The plan names what will be done, in what order, and what will not be done.

**Healthy signal:** The plan is directly traceable to the confirmed motivation from Step 3. The user can evaluate it without asking follow-up questions.

**Deviation — User did not verify the plan is grounded in their confirmation:** The user reads the plan for correctness but not for whether it reflects their confirmed motivation. A plan can be technically sound and motivationally wrong. Fix: before accepting, ask: does this plan follow from what I said in Step 3?

**Deviation — User accepted a vague plan:** The plan did not specify what would and would not be done, and the user accepted it anyway. The ambiguity is now loaded into Step 6. Fix: if a plan doesn't name concrete artifacts and scope boundaries, ask Claude to make it specific before accepting.

**Deviation — User accepted an over-scoped plan:** The plan included work not traceable to the confirmed motivation, and the user did not trim it. Fix: strike scope that doesn't follow directly from Step 3 before accepting.

---

### Step 5 — Plan Confirmed

The plan is confirmed — either by the user explicitly accepting it, or by Claude proceeding when the Setup Phase has predefined the correct approach with no discretionary decisions remaining.

**User confirmation is required when:** the plan involves judgment calls not resolved by repo patterns, the scope has any ambiguity, or accuracy would be at risk if Claude proceeded without explicit sign-off. User confirmation here is the safeguard against executing a plan the user cannot evaluate.

**Claude proceeds without explicit acceptance when:** the Setup Phase defined repo-specific coding practices and patterns that fully determine the plan, and no discretionary choices remain.

**Healthy signal:** One-turn acceptance or a pattern-resolved proceed. "Yes", "go ahead", "looks good" — or Claude states the plan is fully determined by established conventions and begins delivery.

**Deviation — User accepted a plan they don't fully understand:** No signal until Step 7 when the delivery surprises them. Fix: if any part of the plan is unclear, ask before accepting — not after seeing the result.

---

### Step 6 — Claude Delivers

Claude executes the accepted plan. Delivery is bounded by the plan — nothing more, nothing less.

**Healthy signal:** Committed changes are directly traceable to the accepted plan.

**Deviation — User did not notice delivery drifted from the plan:** Claude added things not in the plan or omitted things that were, and the user approved it anyway. The plan was the contract; reviewing against the plan is the user's job in Step 7. Fix: review the diff against the plan, not just for correctness.

**Deviation — User did not intervene when delivery stalled:** Claude hit an obstacle and the user waited rather than re-entering the design loop. Fix: if delivery stops or changes approach silently, call it out and return to Step 4 for a revised plan.

---

### Step 7 — User Reviews and Approves

The user reviews the delivered work against the accepted plan and the confirmed motivation. If both match, they approve.

**Healthy signal:** Approval in one review pass. No rework requested.

**Deviation — User reviewed for correctness but not for fit:** The code works but doesn't do what the user actually needed. The acceptance criteria were the confirmed motivation and the accepted plan — not just "does it run." Fix: review against Step 3 and Step 4, not just against Step 6's output.

**Deviation — User approved without reviewing:** Approved because delivery looked plausible, not because it was verified. Shows up later as a follow-up issue. Fix: treat every approval as a statement that the loop is closed — because it is.

---

## The Fundamental Failure Mode

Every deviation in this workflow is a variation of one thing:

> **A gap between what the user expected Claude to deliver and what Claude actually delivered.**

That gap is not fixed by reacting after Step 6. It is fixed by adjusting the preconditions *before* Step 6 — everything in Phase 0 through Step 5 exists for exactly this purpose. The entire design phase is precondition management.

When the gap appears, the question is never "why did Claude do that." It is always: **which precondition was missing or wrong?**

- Setup Phase broken → Claude lacked context before the session started
- Step 1 underspecified → Claude engaged with the wrong problem
- Step 2 skipped or wrong → motivation was assumed, not confirmed
- Step 3 unresolved → the anchor for the plan was ambiguous
- Step 4 accepted without verification → the plan encoded the wrong scope or fit
- Step 5 accepted without understanding → the user signed off on something they couldn't evaluate

Each of these is a precondition the user controls. Fixing the gap means identifying which precondition failed and changing the user's behavior at that step going forward.

---

## Signals

Steps 1–5 are the design phase. Time spent here is not a problem. What matters is steps being skipped or repeated, and rework in the execution phase.

| Signal | Root phase | User behavior it reflects | Healthy value |
|--------|-----------|--------------------------|---------------|
| Missing session data | Setup Phase | Hook not registered | 0 gaps |
| Tool call failure rate | Setup Phase or Step 6 | Environment not set up correctly | 0 per session |
| User re-supplying context each session | Setup Phase | CLAUDE.md not maintained | 0 per session |
| Re-prompt count | Design (1–2) | User repeated rather than added context | 0 per session |
| Bail-out sessions | Design (1–3) | User's motivation was unresolved at session start | 0 per week |
| Push count on PR | Execution (6–7), root in Design | User accepted a plan without verifying fit | 0–1 post-review |
| Review comment density | Execution (7), root in Design | User approved delivery without reviewing against plan | Trending toward 0 |
| Session → issue → PR chain completeness | All | Work started outside the workflow | 1:1:1 ratio |

**The primary metric is execution-phase rework.** But it is a lagging indicator — look one phase upstream from the symptom to find the user behavior that caused it.

---

## Experiment Hypothesis Template

Experiments are framed as user behavior changes:

> *"In sessions where [observed user behavior], [metric] was [N]% higher than baseline. Hypothesis: [specific user behavior change] will reduce [metric] to [target value]. Intervention: [concrete thing the user will do differently]. Measure: [named field in sessions.db or github.db]. Checkpoint: [date]."*
