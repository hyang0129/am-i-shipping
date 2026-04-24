# am-i-shipping — Constitution

## Thesis

The product is the actionable finding — what the user can change in their setup, prompts, plans, or review habits to make their next agentic coding session better-aligned with the idealized workflow. Findings range from clean prescriptive recommendations to "investigate further" pointers, because the project will not ship a recommendation it is not confident in. The delivery surface is the pipeline that ingests git history and Claude Code logs into a database and synthesizes a periodic markdown retrospective.

---

## Laws

## Law 1 — The product reaches the user only through channels the user has solicited; it never delivers content through any push, schedule, gate, or auto-applied edit

**Why:** An unsolicited message — notification, scheduled email, gating prompt, auto-applied config edit — hits the user when they did not invite it. They feel guilt (busy week, missed the contract), annoyance (interrupted), or judged (the product is asserting itself over them). Each unsolicited contact teaches the user that engaging with the product means absorbing those feelings, so the user starts ignoring the product preemptively. By the time a finding actually matters, the user is already in the habit of dismissing the channel that carries it. Adherence collapses; the project's whole point fails. The mitigation is not "make the push gentler" — any unsolicited contact, however minor, trains the avoidance reflex.

**Rejected Alternative:** Any system that delivers judgment without explicit user solicitation. This includes the obvious push forms (notifications, scheduled emails, in-IDE nudges), the gating forms (Braintrust's enforce-before-ship pattern, where the product blocks an action until acknowledged), the auto-edit forms (agent-retro's apply-config-changes pattern, where the product modifies user-controlled state without being asked), and the comparative forms (team rollups, peer benchmarks, leaderboards). The "make it ignorable" defense fails because even an easily-ignored notification introduces the *flow* of ignoring the product, which generalizes.

**Anti-pattern:** A cron job that emails the user "you haven't run a retrospective in 14 days." A VS Code extension popup at session-end suggesting the user reflect. An auto-applied edit to `CLAUDE.md` based on detected patterns. A section in the report headed "compared to other developers." A confirmation dialog on Claude Code launch saying "review last week's findings before proceeding."

**Scope:** All channels through which any artifact produced by the project reaches the user. Carve-in: when the user opens the retrospective themselves, judgment may appear inside the document — the act of opening is the solicitation.

**Detector:** No clean grep pattern; this is reasoning-about-intent. Reviewer test: "Does any new code path cause the project to *reach* the user before the user has reached for it?"

---

## Law 2 — The project extracts nothing from the user; no money, attention, telemetry, identity, lock-in, or required dependency

**Why:** The project's purpose is to help the user. Any extraction mechanism — money, attention, data, dependency — introduces a competing motive that corrodes the helping motive over time. A monetized version has to optimize for whatever drives revenue (engagement metrics, retention, upsell), which structurally conflicts with Law 1 (you cannot simultaneously "never push" and "drive retention"). A telemetry-collecting version builds an asymmetry of knowledge that erodes trust: the project knows what the user is doing, the user does not know what the project knows or where it goes. A dependency-creating version (remote services, vendor lock-in) gives the project leverage over the user that compounds over time and makes leaving costly.

There is also a structural conflict between extraction and the project's goal. The user benefits when their workflow improves to the point that they need the project less. An extracting project loses revenue, attention, or data flow when that happens, so it gains incentive to keep the user dependent. The thesis admits the possibility — even welcomes the possibility — of the user outgrowing the tool. Extraction makes outgrowing the tool an adversarial event for the project, which warps every incentive downstream of that. The simplest defense is the strictest: extract nothing, and the conflict cannot arise.

**Rejected Alternative:**
1. **Closed SaaS with paid tier** (LangSmith, Braintrust as commercial observability products). The weak form of the rejected alternative — easy to refute on privacy and lock-in grounds.
2. **OSS-core / dual-license sustaining business** (Continue.dev's open-source-with-commercial-support model; LangSmith/Braintrust's OSS-observability + SaaS-eval split). The stronger real opposite. Refuted because even these models extract attention (the user is aware of the commercial entity and may be solicited later), create a dependency on the company's continued operation (if it folds, the project's evolution stops), and introduce engagement metrics into the project's evolution. "We want literally nothing from the user" rules out the OSS-core form just as cleanly as the closed-SaaS form.

**Anti-pattern:** A "donate" button at the bottom of a retrospective. A telemetry SDK that reports usage to a remote server. A "create an account to unlock X" gate. A premium tier ("extended history," "weekly digest email"). A LICENSE that restricts commercial use of forks. A required call to a remote API the user does not already have a relationship with (the project should be runnable entirely on the user's own infrastructure). A README badge linking to the author's Patreon. A newsletter signup form. A "tell us what you'd like to see next" survey baked into the product.

**Permitted (to draw the line):** The user *voluntarily* contributing code, filing an issue, telling other people about the project, or sponsoring the author through a separate channel they sought out themselves. Contributions are gifts initiated by the user to the project, not extractions by the project from the user. The project may accept gifts freely given; it may not solicit them.

**Scope:** Project distribution, the user-facing product, and any data flows the project initiates. Excludes the user's own existing infrastructure (Claude Code subscription, local git, local LLM credentials) — those are the user's resources being used by the project on the user's behalf, not extraction.

**Detector:** Reviewer test — for any change: (1) "Does this introduce a flow of value from the user to the project (money, attention, data, identity, dependency)?" Yes → violation. (2) "Does this create an obligation the user has to the project?" Yes → violation. (3) "Does this introduce a remote service the project depends on, that is not the user's own existing service?" Yes → violation.

---

## Law 3 — The project has exactly one goal: the delta between what the user expected the agent to deliver and what the agent actually delivered. The goal sentence is permanent; other dimensions exist only to inform it

**Why:** The fixed-and-singular design exists to keep the project's measurement instrument trustworthy and its identity legible.

If the goal drifts, trends become meaningless: a user comparing month 7 to month 1 cannot tell if their adherence improved, declined, or stayed flat — the comparison is corrupted by redefinition. Worse, an evolvable goal invites the worst failure mode in measurement: softening the standard when reality fails to match it. The goal exists to be the thing reality is held against, not the thing that adjusts when reality disappoints. Once the principle can be revised in response to observed findings, the project becomes a mirror that reshapes itself to flatter whatever the user is doing — useless as a measurement instrument.

If other metrics get surfaced as goals in their own right rather than as inputs to the primary one, every additional metric becomes another question the user is implicitly asked to answer. Each new question carries the judgmental dynamics Law 1 protects against (am I fast enough, clean enough, prolific enough), eroding the receptivity the project depends on. The project also loses its identity: velocity, code quality, team performance are already well-served by LinearB, Jellyfish, DORA, and others. Promoting any secondary metric to goal status turns this into a less-good version of tools that already exist.

**Rejected Alternative:**
1. **Versioned/evolvable goal** (DORA's yearly research revisions; DX Core 4's evolving dimensional model; SPACE's deliberate under-specification). Refuted: evolution destroys trend comparison and creates the goalpost-moving failure mode.
2. **User-derived goal** (Jellyfish's OKR-anchored model: derive the goal per-user from the user's own stated objectives, with explicit Goodhart-avoidance reasoning). Refuted: (a) Jellyfish requires an org structure that doesn't exist for a single-user tool; (b) the goal here *is* the user's intent — using the project is consent to it, no derivation needed; (c) Goodhart is moot because the project ships no performance targets.
3. **Multi-dimensional goals** (a primary expected-vs-delivered metric alongside secondary velocity/quality/volume metrics, each with its own findings). Refuted: every additional reportable metric is another question the user is asked to answer, eroding receptivity, and dilutes the project into "yet another engineering-analytics tool."

**Anti-pattern:** A retrospective section headed "Velocity: −20% week-over-week" — velocity surfaced as its own finding. A recommendation reading "increase shipping velocity to match Q2." A code-quality scorecard section in the report. Importing DORA or SPACE dimensions as additional reportable anchors. Modifying the goal sentence to "the agent delivers what the user expected, eventually" or "…most of the time." Adding a numerical adherence target ("aim for 95%") that turns the goal into a metric to game. Reading the current week's findings, deciding they're harsher than expected, and softening the operational definition of "expected" to make them gentler.

In-scope examples (so the line is clear): "Sessions where you wrote a plan before invoking the agent had a 70% smaller expected-vs-delivered gap than sessions where you didn't" — planning-time used as an explanatory variable for the primary delta. "On weeks with >5 active sessions, the gap widened 30% — try fewer, longer sessions" — session count used to explain the delta. Internal tracking of any quantity that informs the primary measurement is fine; the test is whether the *finding* names the quantity as an end or as a means.

**Scope:** The goal sentence and the contents of published findings. Operational machinery — how the project measures the goal — is allowed to evolve, provided the evolution serves the same goal more accurately.

**Detector:** Reviewer test — for any new metric or finding: (1) "Does this name a quantity as a goal in its own right, or use it to explain the primary expected-vs-delivered delta?" Former → violation. (2) "Does this change weaken or qualify the goal sentence to better fit observed findings?" Yes → violation. (3) "Does this introduce a numerical performance target the user is expected to hit?" Yes → violation.

---

## Law 4 — Every recommendation targets a surface under the user's control; the project never proposes changes that require modifying the agent

**Why:** A finding the user cannot act on is not an actionable finding — it is a complaint. The thesis names the project as "the actionable finding"; shipping complaints collapses the value proposition. The concrete failure: user opens retrospective, finding reads "the agent truncated your spec at 8K tokens — this caused the divergence." User agrees and can do nothing — they don't control Claude's tokenizer. They learn that opening the retrospective yields agreement-without-agency, which is the worst possible product experience: confirmation that something is wrong combined with confirmation that they can't fix it. Receptivity collapses by the same route Law 1 protects against, just from a different direction. The wider field already does the agent-side work (Cody intent-inference, Continue.dev "tool adapts to you", agent-retro skill edits); this project's distinctive value is the user-side work no one else does. Diluting into the agent-side space costs the differentiator and produces nothing the user can act on.

**Rejected Alternative:** Cody's intent-inference model — the *system* continuously infers user intent from behavior and adapts the agent's responses via RAG, so the user never has to change how they prompt. The strongest opposite stance because it accepts user heterogeneity as a feature to design around rather than coach. Refuted on three grounds: (1) it requires owning an agent harness, which this project does not; (2) it solves a different problem — making the agent more responsive to whatever the user typed — but the project's normative principle is *agent delivers what user expected*, which fails at the expression layer (user did not communicate the expectation), not at the inference layer (agent did not infer it well enough); (3) even if a wrapper could be built, it would require recommending the user *install* it, which is a user-behavior change (choice of tooling) — and that recommendation is in scope without the project itself owning the wrapper.

**Anti-pattern:** A finding reading "Claude's context window prioritized recency over relevance — increase the window size." A finding reading "the harness retried 3 times silently, masking the underlying error." A built-in component that intercepts Claude API calls and pre-processes prompts to "fix" them. A built-in component that trains on the user's session history to auto-tune system prompts. A finding reading "switch from Sonnet to Opus for tasks of type X" *if* phrased as "the project requires you to" — the user-choice form ("you may want to try Opus on type X") is in-scope; the project-mandate form is not.

In-scope examples (so the line is clear): "Your prompts in failed sessions don't specify a success criterion — add a 'done means' line." "No CLAUDE.md in this repo — consider adding one with [X]." "Sessions where you handwrite specs go better than ones where you rely on the agent to infer — try the planning skill." All target user-controllable surfaces.

**User vs. agent definitions:** "User" includes the user's own habits (prompts, plans, review behavior) and every configuration surface they own — `CLAUDE.md`, `settings.json`, hooks, skill installation, choice of agent/harness/IDE, prompt templates. "Agent" means everything outside the user's control — model weights, model behavior, vendor-side defaults, harness internals.

**Scope:** All recommendations and prescriptions in the published findings.

**Detector:** Reviewer test — "Is the change this recommendation requires under the user's control? If no → violation. If the recommendation is 'install or switch to tool X', is it phrased as a *user choice* rather than a project mandate? If no → violation."

---

## Law 5 — The project ships an actionable finding only when confidence is high enough that it can be stated as a clean recommendation; otherwise it ships only an "investigate further" pointer

**Why:** A wrong recommendation — even one labeled low-confidence — costs the user trust in the project. Once trust degrades, the user has to evaluate every subsequent finding ("is this another bad one?"), which means doing their own investigation on top of the project's work and defeats the point of the project. Labeling confidence does not repair this: users don't calibrate to scores — they treat them as act-or-ignore, and a single bad call erodes credibility regardless of the label it carried. Reversibility (ship the edit, let the user roll it back) doesn't repair it either, because the trust damage happens at the *moment of recommendation*, before execution. The only durable defense is to under-ship: fewer findings, all of them reliable. Over-promise → under-deliver consistently produces the same trust collapse Law 1 guards against, by a different route.

**Rejected Alternative:**
1. **Confidence-scored prescriptions** (probability sliders, "high/medium/low" tags) — refuted because users don't calibrate to scores; they binarize, and a single miscalibration burns the same trust as an unlabeled wrong call.
2. **Reversible-prescription model** (agent-retro: ship the literal edit, make acceptance interactive, bound the cost via reversibility) — refuted because reversibility bounds the *operational* cost of a wrong call but not the *credibility* cost. The user has already weighed the recommendation by the time they reverse it.

**Anti-pattern:** A finding header reading "Confidence: 0.7." A recommendation phrased "you might want to consider…" A diff with accept/reject buttons proposing a workflow edit. A ranked list of N candidate findings the user is expected to filter by score. A retrospective entry that says "if you have time, look into…" — that's a hedge masquerading as an investigate-further pointer.

**Scope:** All findings published to the user (i.e., the retrospective). Intermediate analytical artifacts (database rows, debug summaries, raw LLM outputs) are not "findings" and are exempt — those are inspection surfaces the user can drill into if they choose, not assertions the project is making.

**Detector:** Grep the published output layer for confidence numbers, hedge phrasings, and accept/reject UI affordances. None should appear.

---

## Rejected Alternatives

Whole-project design choices that were evaluated and rejected:

- **Realtime/inline IDE intervention as the product surface** (Cursor, Copilot, Penn State prompt-coaching). Rejected: it is the push form Law 1 forbids and produces measured user frustration even when the targeted outcome improves.
- **Team-aggregated analytics product** (LinearB, Jellyfish, Swarmia). Rejected: the project is single-user by design ("am-I-shipping," not "are-we-shipping") and team aggregation introduces external judgment Law 1 protects against.
- **Generic engineering-analytics framework** (DORA, SPACE, DX Core 4). Rejected: the project measures one thing — expected-vs-delivered — not a multi-dimensional productivity surface (per Law 3).
- **For-profit SaaS distribution** (LangSmith, Braintrust commercial). Rejected per Law 2; restated here as a project-shape decision because the OSS-only posture cascades into licensing, hosting, infrastructure, and contribution policy.
- **Multi-source data ingestion as the default ingestion stance** (Jellyfish ingests OKRs/tickets/calendars/PRs). Rejected as default; the project ingests git history + Claude Code session logs only. Expansion permitted when a new source demonstrably improves expected-vs-delivered analysis.
- **AI/agent tuning as the value proposition** (Cody intent-inference, Continue.dev "tool adapts to you," agent-retro skill edits). Rejected per Law 4; the project's distinctive value is the user-side work no peer does.
- **Investigation tooling as a separate first-class product scope.** Rejected as separate scope — already implicit. Intermediate analytical artifacts (database, summaries, raw outputs) serve the investigation function; no separate investigator surface needed.
- **LLM-driven prescriptive recommendations without confidence guardrails** (agent-retro literal config edits). Rejected per Law 5.

---

## Review Heuristic

For any proposed feature, code change, or design decision:

1. Does this satisfy **Law 1** (the project never reaches the user except through channels the user has solicited)?
2. Does this satisfy **Law 2** (the project extracts nothing from the user — money, attention, telemetry, identity, lock-in, or required dependency)?
3. Does this satisfy **Law 3** (the project's only goal is the delta between expected and delivered; other dimensions inform but do not become goals)?
4. Does this satisfy **Law 4** (every recommendation targets a surface under the user's control; the project never proposes changes that require modifying the agent)?
5. Does this satisfy **Law 5** (recommendations ship only at high confidence; "investigate further" otherwise — never confidence scores or hedges)?

If any answer is "no" or "unclear" — the feature requires redesign, not a carve-out.
