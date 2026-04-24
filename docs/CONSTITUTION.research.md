# Constitution Research

_Generated: 2026-04-24. Thesis hash: The product is the actionable finding —._

## Design space overview

The problem space — turning developer/agentic-session activity data into behavior change — is occupied by three loosely separable schools. The **engineering-intelligence school** (LinearB, Jellyfish, Swarmia, Code Climate Velocity, Sleuth) ingests git/PR/issue data and surfaces metrics + benchmark comparisons, leaving prescription mostly to the human manager; their published reasoning leans heavily on Goodhart-avoidance and "metrics start conversations, not actions." The **LLM/agent observability school** (LangSmith, Braintrust, agent-retro) instruments AI sessions specifically and trends toward higher-confidence, more prescriptive output because it controls eval ground truth — Braintrust gates production releases, agent-retro proposes literal config edits. The **realtime/in-IDE school** (Copilot, Cursor, Continue.dev, Penn State's "inclusive prompt coaching" research prototype) intervenes in the moment of authorship rather than after the fact, and has explicit findings on the friction cost of doing so.

Cutting across those schools are stances on the normative anchor (research-derived like DORA, deliberately under-specified like SPACE, prescriptive like DX Core 4, or strategy-derived like Jellyfish's OKR alignment), and on whether the unit of improvement is the human (Swarmia's developer-autonomy framing, the prompt-coaching research) or the tool (Continue.dev's "tool adapts to you," Cody's RAG-over-fine-tuning stance, agent-retro's edit-the-skill output).

## Per-debate entries

---

### Confidence threshold for shipping recommendations — high-bar/investigate-further vs. confidence-scored prescriptions

**Stance A** (high bar; surface signals + investigation pointers; only ship a recommendation when sure):
- Project: Code Climate Velocity — explicitly frames metrics as conversation starters that require pairing with qualitative input before action; "you'll need to pair findings with conversations with team members to get a full understanding of what causes metrics to change." Source: https://docs.velocity.codeclimate.com/en/articles/2751968-are-these-metrics-actionable
- Project: Swarmia — positions data as fuel for developer-led inquiry rather than top-down prescription; their stated philosophy is that "making engineering data accessible to developers themselves, not just their managers, creates healthier team dynamics." Source: https://www.swarmia.com/engineering-effectiveness/

**Stance B** (ship prescriptive, machine-editable recommendations):
- Project: agent-retro — produces "specific edits to skills, rules, or config. Not vague 'improve X' — the actual text to change," and walks the user through accepting them. Source: https://github.com/giannimassi/agent-retro
- Project: Braintrust — gates production via evals and "enforces release standards before changes ship," treating eval scores as actionable thresholds rather than discussion fodder. Source: https://www.braintrust.dev/articles/langsmith-vs-braintrust

**Is the opposite stance defensible?** Yes — when the system controls the artifact being changed (a config file, a prompt, a release gate), the cost of a wrong prescription is bounded and reversible, so the calculus shifts toward action. am-i-shipping changes *user behavior*, where wrong prescriptions are costlier and less reversible, but agent-retro shows that even agent-side prescriptions can be made safe via interactive walkthroughs.

**Strongest opposite-stance argument found:**
> "Every agent framework has inline reflection (retry loops, critic agents). None of them do post-session systemic reflection... [output is] specific edits to skills, rules, or config. Not vague 'improve X' — the actual text to change." — agent-retro README, https://github.com/giannimassi/agent-retro

---

### User behavior vs. tool behavior as unit of improvement — fix the human-in-the-loop vs. tune the agent

**Stance A** (improve the human; treat the AI as fixed):
- Project: Penn State / Oregon State "inclusive prompt coaching" — intervention targets the user's prompt-writing behavior, not the model; the measured outcome was user awareness and confidence, not model output quality directly. Source: https://www.psu.edu/news/research/story/prompt-coaching-tool-raises-user-awareness-bias-generative-ai-systems
- Project: Swarmia — treats developer practices and team workflow as the lever ("workflow," "WIP limits," "flow efficiency"), not the tools developers use. Source: https://www.swarmia.com/blog/flow-efficiency/

**Stance B** (tune the tool/agent; treat user behavior as fixed):
- Project: Sourcegraph Cody — explicitly chose RAG over fine-tuning so the *system* adapts to the codebase and to inferred user intent in real time, rather than asking the user to change how they prompt; "as a user is typing, Cody constantly evaluates the intent of the actions a user is taking." Source: https://sourcegraph.com/blog/anatomy-of-a-coding-assistant
- Project: Continue.dev — frames its philosophy as "the tool adapts to you," with user-defined rules, context providers, and model choice as the surface for change. Source: https://github.com/continuedev/continue
- Project: agent-retro — its retrospective edits *the agent's* skills/rules/config, not the user's habits. Source: https://github.com/giannimassi/agent-retro

**Is the opposite stance defensible?** Yes, and it is the dominant stance in the AI-coding-assistant space. The argument is that the model and harness are the controllable surface (you ship code there); the user is heterogeneous, slow to change, and won't read your weekly markdown. am-i-shipping's choice to fix the human is unusual and worth defending explicitly.

**Strongest opposite-stance argument found:**
> "Cody programmatically retrieves context when a user submits a request and then uses RAG... As a user is typing, Cody constantly evaluates the intent of the actions a user is taking, using Tree-Sitter to identify what type of completion experience will fit best into the active workflow." — Sourcegraph anatomy-of-a-coding-assistant post, https://sourcegraph.com/blog/anatomy-of-a-coding-assistant

---

### Normative anchor — idealized workflow as yardstick vs. user-derived or no anchor

**Stance A** (pre-defined normative workflow as the yardstick):
- Project: DX Core 4 — most prescriptive of the major frameworks, ships a fixed dimensional model (Speed, Effectiveness, Quality, Impact) with specific recommended metrics. Source: https://www.swarmia.com/blog/comparing-developer-productivity-frameworks/
- Project: DORA — research-derived "what good looks like" capabilities that organizations are measured against. Source: https://www.swarmia.com/blog/comparing-developer-productivity-frameworks/

**Stance B** (deliberately non-prescriptive; let the org/user define what matters):
- Project: SPACE framework — explicitly refuses to prescribe specific metrics, naming only dimensions; "SPACE identifies what dimensions matter but provides almost no guidance on what specific metrics to track." Source: https://www.swarmia.com/blog/comparing-developer-productivity-frameworks/
- Project: Jellyfish — anchors evaluation to the *organization's own OKRs* rather than a fixed workflow model: "connects engineering activity, like pull requests, commits, and tickets, to business objectives." Source: https://jellyfish.co/blog/how-manomano-aligns-engineering-delivery-with-strategic-goals-okrs/

**Is the opposite stance defensible?** Yes. The Goodhart-avoidance argument (Jellyfish's own blog post on it) is that imposing a fixed normative model invites gaming when the model doesn't match the user's actual goals; deriving the anchor from the user's stated OKRs aligns measurement with intent. The counter, which the thesis implicitly takes, is that a user who could articulate their workflow well wouldn't have the misalignment problem in the first place.

**Strongest opposite-stance argument found:**
> "Every metric you track will eventually be gamed if you're not careful... The difference between those that successfully integrate metrics into their engineering operations and those that stumble largely centers around the ability to mitigate the effects of Goodhart's Law." — Jellyfish, Goodhart's Law in Software Engineering, https://jellyfish.co/blog/goodharts-law-in-software-engineering-and-how-to-avoid-gaming-your-metrics/

---

### Cadence — periodic batch synthesis vs. realtime/inline feedback

**Stance A** (periodic batch retrospective):
- Project: agent-retro — "post-session systemic reflection" run at the end of a session, explicitly contrasted with inline reflection. Source: https://github.com/giannimassi/agent-retro
- Project: Range — async written check-ins + scheduled retrospectives; "everything's written down so it's easy to find and reference," with retros as a deliberate weekly/biweekly ritual. Source: https://www.range.co/help/article/how-to-improve-your-retrospective-using-range
- Project: Sleuth — periodic Reviews with AI-generated summary and scoring as the synthesis surface. Source: https://www.sleuth.io/post/dora-metrics/

**Stance B** (realtime/inline intervention at the moment of authorship):
- Project: Penn State "inclusive prompt coaching" — issues warnings as the user types the prompt, before submission. Source: https://www.psu.edu/news/research/story/prompt-coaching-tool-raises-user-awareness-bias-generative-ai-systems
- Project: Cursor — predictive "tab to accept" inline edits at the cursor; intervention is in the typing loop. Source: https://learn.ryzlabs.com/ai-coding-assistants/cursor-vs-github-copilot-the-pros-and-cons-in-2026

**Is the opposite stance defensible?** Yes, but the Penn State research surfaced the explicit cost: realtime nudges produced measurable user frustration ("a slap on the wrist") and false-positive flags on innocent inputs, even when they improved the targeted outcome. Their proposed mitigation — make it toggleable and context-aware — is what a batch system avoids by construction.

**Strongest opposite-stance argument found:**
> "Participants in the inclusive and detailed prompt coaching conditions reported a more frustrating user experience compared to those in the no coaching condition, and perceived it as less helpful and more frustrating compared to control conditions." — Penn State news release, https://www.psu.edu/news/research/story/prompt-coaching-tool-raises-user-awareness-bias-generative-ai-systems

---

### Investigation tooling in scope — ship investigators alongside analysis vs. pure analysis surface

**Stance A** (ship investigation tools as part of the product, hedge against low-confidence prescription):
- Project: Sleuth — bundles investigation surfaces (bottleneck visibility, AI-summarized data, pre-filled review templates) alongside metric tracking, so the user can drill from the score into the why. Source: https://www.sleuth.io/dora/
- Project: Braintrust — pairs production traces, eval datasets, and CI quality gates in one workflow; failures become regression tests, i.e. the investigation artifact is a first-class output. Source: https://www.braintrust.dev/articles/langsmith-vs-braintrust

**Stance B** (pure analytical/reporting layer; investigation is downstream/manual):
- Project: Code Climate Velocity — produces metrics and explicitly hands investigation back to humans: "you'll need to pair findings with conversations with team members." Source: https://docs.velocity.codeclimate.com/en/articles/2751968-are-these-metrics-actionable
- Project: SPACE-style frameworks — define the dimensional model and stop there; what to instrument and how to investigate is the implementer's job. Source: https://www.swarmia.com/blog/comparing-developer-productivity-frameworks/

**Is the opposite stance defensible?** Yes — a pure-analysis stance keeps the surface area small and avoids the trap of shipping a half-built investigator that competes with users' real debugging tools. The user has already flagged that they may need investigators because automation can't be trusted to recommend; the defensible counter is that this is scope creep and the report itself should be designed so the user can investigate manually.

**Strongest opposite-stance argument found:**
> "You'll need to pair findings with conversations with team members to get a full understanding of what causes metrics to change, and qualitative and quantitative data together will help you determine the best way to take action." — Code Climate Velocity docs, https://docs.velocity.codeclimate.com/en/articles/2751968-are-these-metrics-actionable

---

## Unsolicited alternatives

- **Single-user vs. team/aggregate analysis**: every cited tool except agent-retro assumes a team; am-i-shipping appears single-user. Live debate: do you reject team aggregation entirely, or design so a future team rollup is possible?
- **Open-source / self-hosted vs. SaaS posture**: Continue.dev makes openness load-bearing ("no black boxes"); LangSmith/Braintrust are SaaS-first. For a tool that ingests private session logs containing arbitrary code and prompts, this is a stance to take explicitly.
- **Storage substrate as a constraint**: the thesis names "a database" generically, but tools in the space split on relational-warehouse (Jellyfish, LinearB) vs. trace-store (LangSmith, Braintrust) vs. flat-file (agent-retro). The choice constrains what kinds of findings are tractable later.
- **Who writes the retrospective — LLM vs. deterministic templating vs. hybrid**: Sleuth and EasyRetro generate via LLM; agent-retro mixes deterministic extraction with LLM proposals. The user's own confidence concern applies most sharply to the LLM step.
- **Feedback loop into the system itself**: none of the cited tools explicitly close the loop — i.e., did the recommendation, when followed, actually improve the next period? A first-class "did the prescription work" measurement is a live debate worth naming.
- **Versioning the normative anchor**: the idealized workflow will evolve. DORA versions its research; SPACE doesn't. Whether am-i-shipping treats `idealized-workflow.md` as a versioned artifact whose changes invalidate prior findings is an architecture-level decision.
