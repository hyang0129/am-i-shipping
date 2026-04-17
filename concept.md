# Workflow Monitor — Design Discussion Summary

*Session date: 2026-04-14*

---

## Context

The user is a developer who:
- Uses Claude Code exclusively (does not write code directly)
- Works via an Issue → PR workflow; all PRs are Claude-generated
- Has 2 Max 20 Claude Code subscriptions
- Wants to increase velocity while maintaining or improving quality

Goal: design a system that monitors work patterns, identifies friction, and proposes improvements — **not** a tool to solve current workflow issues, but one that observes and learns over time.

---

## The Team

Six personas contributed perspectives in parallel:

| Persona | Background | Usage level |
|---|---|---|
| Alex | Startup CTO/tech lead | 10x |
| Jordan | FANG Senior Staff SWE | 10x |
| Sam | Anthropic engineer (insider) | 10x |
| Riley | FANG mid-level SWE | 1x |
| Casey | Series A startup full-stack dev | 1x |
| Morgan | LLM prompt engineering expert | — |

---

## Key Signals to Track

### Friction (Claude Code sessions)
- **Re-prompt rate** — same intent restated 3+ turns in one task = breakdown (Alex)
- **Tool call failure rate** — repeated failures indicate environment or task underspecification (Alex)
- **Re-read of same file within a session** — context pollution or mid-task redirection (Sam)
- **Turn count per logical task** — 3–5 turns = efficient; 8+ = something broke (Sam)
- **Bail-out sessions** — high rephrase count + zero committed lines (Casey)

### Velocity
- **Issue throughput** — issues created vs. closed per week
- **Session → issue correlation** — map sessions to issues via branch name/working dir
- **Commit correlation** — session active within 30 min before commit = productive session proxy (Alex)

### Quality
- **PR review cycle count** — number of comment → push → re-review rounds (Jordan, Riley)
- **Review comment density** — comments per 100 lines changed (Riley)
- **Issue abandonment rate** — opened >14 days ago, no linked PR, no activity (design decision)
- **Backlog growth trend** — net issues created minus closed week-over-week

### Structural patterns (GitHub — most important given Issue → PR workflow)
- What types of issues get created (bug, feature, debt, research)
- Which types get resolved vs. which age out and die
- PR-to-issue ratio — are PRs being opened without issues? (ad-hoc work escaping the system)
- Issue age at resolution — how long does work actually take end-to-end?
- Backlog health — open issues by age cohort

### App activity (behavioral proxy)
- Post-response app-switch: staying in VSCode = answer integrated; switching to browser = spawned follow-up questions (Riley)

---

## What NOT to Measure

- **Session count / token usage** — vanity metrics; engagement with the tool, not value produced (Morgan)
- **Raw line counts** — meaningless without change-type context (Jordan)
- **Population benchmarks** — "you used Claude 23% more than average" is corrosive; personal baseline only (Jordan)
- **Real-time dashboard** — creates Goodhart's Law; weekly cadence is deliberate (Morgan)

---

## System Architecture

### Data Collection (fully passive — no manual tagging)

| Source | How | Output |
|---|---|---|
| Claude Code sessions | Post-session hook parsing `~/.claude/projects/<hash>/` | `data/sessions.jsonl` |
| GitHub issues + PRs | Nightly `gh` CLI poll | `data/github.jsonl` |
| App activity | PowerShell watcher, 30s intervals, Task Scheduler | `data/appswitch.jsonl` |

No privacy constraints — full session transcripts accessible for analysis.

### Synthesis Engine (weekly, automated)

Runs Sunday evening via Task Scheduler. Joins the three data sources into a structured summary, then calls the Claude API with a fixed prompt:

```
You are analyzing one developer's workflow data for the past week.
Data: [structured JSON summary]
Baseline: [developer's own prior 4-week averages]

Do NOT produce scores or rankings. Instead:
1. Identify at most 2 anomalies against the developer's own baseline.
2. For each anomaly, offer three possible explanations — one environmental,
   one behavioral, one tool-related.
3. Ask at most 2 clarifying questions total (across all anomalies) the developer can answer from memory.
4. Do not recommend anything yet.
```

Output is written as a Markdown file in `retrospectives/YYYY-MM-DD.md`. The developer adds their responses to the clarifying questions in the same file.

### Recommendation + Experiment Loop

After the developer responds, a second Claude call generates an experiment spec:

```yaml
id: exp-2026-04-14-001
hypothesis: "Sessions without reading CLAUDE.md have higher re-prompt rates"
intervention: "Read project CLAUDE.md explicitly at session start for 2 weeks"
measure: rephrase_count per session (sessions.jsonl)
baseline: 2.3 rephrases/session
target: <1.5 rephrases/session
checkpoint: 2026-04-28
outcome: pending
```

**Constraints:**
- Max 2 active experiments at any time
- New experiment cannot open until an existing checkpoint is logged
- Every experiment must name a specific measurable field + numeric target
- "Be more intentional" is not an experiment

---

## Implementation Phases

**Phase 1 — Passive collection (weeks 1–3)**
Deploy the three collectors. Collect without analyzing. Establish 4-week baseline before synthesis begins. Do not look at the data while calibrating.

**Phase 2 — Weekly synthesis (weeks 4–8)**
Activate Sunday synthesis. First two cycles: read-only (answer clarifying questions but open no experiments). Tune the synthesis prompt to actual data shape.

**Phase 3 — Experiment loop (week 9+)**
Open the first experiment. Run 2 weeks, log the checkpoint outcome, feed it into the next synthesis cycle as context. The experiment history becomes the most valuable synthesis input over time.

**Definition of done:** an experiment closes with a confirmed behavioral change that persists after the experiment ends.

---

## Repo Structure (when ready to implement)

```
workflow-monitor/          ← its own repo
  collector/
    session_parser.py      # parses ~/.claude/projects/<hash>/ post-session
    github_poller.py       # gh CLI wrapper — issues, PRs, labels, timestamps
    appswitch_watcher.ps1  # PowerShell background logger
  synthesis/
    weekly.py              # joins data, calls Claude API
    prompts/
      synthesis.md         # synthesis prompt template
      experiment.md        # experiment generation prompt
  experiments/
    active/                # YAML specs for in-progress experiments
    closed/                # completed with outcome logged
  retrospectives/          # weekly Markdown files; developer writes responses here
  data/
    sessions.jsonl
    github.jsonl
    appswitch.jsonl
  config.yaml              # repos to monitor, session path, API key ref
```

---

## Prior Art — Existing Repos to Build On

*Researched 2026-04-14.*

### Top 3

| Repo | Stars | What to reuse |
|---|---|---|
| [ccusage](https://github.com/ryoppippi/ccusage) | 12.9k | Best reference for `~/.claude/projects/<hash>/sessions/<uuid>.jsonl` parsing — turn counts, token costs, cache breakdowns. Don't write the session parser from scratch. |
| [ActivityWatch](https://github.com/ActivityWatch/activitywatch) | 17.3k | Cross-platform app-switch + idle detection with local REST API. A PowerShell watcher can POST events into it. Eliminates the need to build app-activity logging from scratch. |
| [disler/claude-code-hooks-multi-agent-observability](https://github.com/disler/claude-code-hooks-multi-agent-observability) | 1.4k | Real-time hook-based capture (12 event types: PreToolUse, PostToolUse, SessionStart/End, SubagentStart/Stop, etc.) with SQLite backend. Complements ccusage — real-time vs. batch. |

### Also Relevant

- [claude-code-log](https://github.com/daaain/claude-code-log) — Python JSONL → Markdown/HTML. Clean Python implementation to study for the collection backend.
- [simonw/claude-code-transcripts](https://github.com/simonw/claude-code-transcripts) — Links sessions to GitHub repos by working directory. Directly relevant to the session → issue correlation join.
- [github/issue-metrics](https://github.com/github/issue-metrics) — Official GitHub Action (Python). Measures time-to-close, time-to-first-response, time-in-label. Adaptable for local `gh` CLI polling.

### Confirmed Gaps (Novel Work)

No existing tool:
- Correlates Claude Code sessions with GitHub **issue** lifecycle (only PR/commit correlation exists)
- Tracks **re-prompt rate** as a signal
- Feeds Claude usage patterns into a **weekly LLM synthesis + experiment loop**

The data collection layer has strong prior art. The synthesis and experiment loop is genuinely new.

---

## Design Principles (consensus)

1. **Passive capture only.** No manual tagging — if it requires discipline, it dies.
2. **Hypotheses, not conclusions.** Synthesis asks questions; it does not prescribe.
3. **Personal baseline only.** Anomalies against your own history, never against population.
4. **Falsifiability required.** Every recommendation must have a named metric and numeric target.
5. **Max 2 active experiments.** Unlimited recommendations = zero recommendations.
6. **Weekly cadence, not real-time.** Real-time visibility optimizes the metric, not the work.
7. **Close the loop.** Recommendations without outcome tracking are just noise.
