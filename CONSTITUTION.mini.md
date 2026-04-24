# am-i-shipping — Constitution (Mini)
<!-- Auto-derived from CONSTITUTION.md — do not edit directly -->

**Thesis:** The product is the actionable finding — what the user can change in their setup, prompts, plans, or review habits to make their next agentic coding session better-aligned with the idealized workflow.

## Laws

### Law 1 — The product reaches the user only through channels the user has solicited; it never delivers content through any push, schedule, gate, or auto-applied edit
**Anti-pattern:** A cron job that emails the user "you haven't run a retrospective in 14 days." A VS Code extension popup at session-end suggesting the user reflect. An auto-applied edit to `CLAUDE.md` based on detected patterns. A section in the report headed "compared to other developers." A confirmation dialog on Claude Code launch saying "review last week's findings before proceeding."
**Detector:** Reviewer test: "Does any new code path cause the project to *reach* the user before the user has reached for it?"

### Law 2 — The project extracts nothing from the user; no money, attention, telemetry, identity, lock-in, or required dependency
**Anti-pattern:** A "donate" button at the bottom of a retrospective. A telemetry SDK that reports usage to a remote server. A "create an account to unlock X" gate. A premium tier ("extended history," "weekly digest email"). A LICENSE that restricts commercial use of forks. A required call to a remote API the user does not already have a relationship with (the project should be runnable entirely on the user's own infrastructure). A README badge linking to the author's Patreon. A newsletter signup form. A "tell us what you'd like to see next" survey baked into the product.
**Detector:** Reviewer test — for any change: (1) "Does this introduce a flow of value from the user to the project (money, attention, data, identity, dependency)?" Yes → violation. (2) "Does this create an obligation the user has to the project?" Yes → violation. (3) "Does this introduce a remote service the project depends on, that is not the user's own existing service?" Yes → violation.

### Law 3 — The project has exactly one goal: the delta between what the user expected the agent to deliver and what the agent actually delivered. The goal sentence is permanent; other dimensions exist only to inform it
**Anti-pattern:** A retrospective section headed "Velocity: −20% week-over-week" — velocity surfaced as its own finding. A recommendation reading "increase shipping velocity to match Q2." A code-quality scorecard section in the report. Importing DORA or SPACE dimensions as additional reportable anchors. Modifying the goal sentence to "the agent delivers what the user expected, eventually" or "…most of the time." Adding a numerical adherence target ("aim for 95%") that turns the goal into a metric to game. Reading the current week's findings, deciding they're harsher than expected, and softening the operational definition of "expected" to make them gentler.
**Detector:** Reviewer test — for any new metric or finding: (1) "Does this name a quantity as a goal in its own right, or use it to explain the primary expected-vs-delivered delta?" Former → violation. (2) "Does this change weaken or qualify the goal sentence to better fit observed findings?" Yes → violation. (3) "Does this introduce a numerical performance target the user is expected to hit?" Yes → violation.

### Law 4 — Every recommendation targets a surface under the user's control; the project never proposes changes that require modifying the agent
**Anti-pattern:** A finding reading "Claude's context window prioritized recency over relevance — increase the window size." A finding reading "the harness retried 3 times silently, masking the underlying error." A built-in component that intercepts Claude API calls and pre-processes prompts to "fix" them. A built-in component that trains on the user's session history to auto-tune system prompts. A finding reading "switch from Sonnet to Opus for tasks of type X" *if* phrased as "the project requires you to" — the user-choice form ("you may want to try Opus on type X") is in-scope; the project-mandate form is not.
**Detector:** Reviewer test — "Is the change this recommendation requires under the user's control? If no → violation. If the recommendation is 'install or switch to tool X', is it phrased as a *user choice* rather than a project mandate? If no → violation."

### Law 5 — The project ships an actionable finding only when confidence is high enough that it can be stated as a clean recommendation; otherwise it ships only an "investigate further" pointer
**Anti-pattern:** A finding header reading "Confidence: 0.7." A recommendation phrased "you might want to consider…" A diff with accept/reject buttons proposing a workflow edit. A ranked list of N candidate findings the user is expected to filter by score. A retrospective entry that says "if you have time, look into…" — that's a hedge masquerading as an investigate-further pointer.
**Detector:** Grep the published output layer for confidence numbers, hedge phrasings, and accept/reject UI affordances. None should appear.

---

If any proposed change violates a law above: redesign required — not a carve-out.
