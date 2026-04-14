# am-i-shipping

This project exists to close one gap: the gap between what the user expected Claude to deliver and what Claude actually delivered. Every deviation, every rework cycle, every stalled session traces back to a precondition that was missing or wrong before Step 6 began. This system collects data, runs weekly synthesis, and proposes experiments — all aimed at adjusting those preconditions so that user expectations and Claude's output align before execution starts. The unit of improvement is not Claude's behavior, which we cannot control. It is the user's behavior: their setup, their prompts, their plans, their review habits.

All analysis is conducted under the idealized workflow paradigm defined in `idealized-workflow.md`. Every signal, anomaly, and experiment hypothesis must be framed in terms of that workflow — which phase it belongs to, which precondition it reflects, and what the user can change.

---

## Workflow Reference

The idealized workflow has three phases. Phase 0 is tool setup — CLAUDE.md current, hooks registered, environment correct — and is a standing precondition, not a session step. The design phase (steps 1–5) begins with the user stating intent, Claude disambiguating motivation by asking whether the user wants X because A or because B, the user confirming, Claude proposing a bounded plan, and the user accepting it. Time in the design phase is investment, not waste. The execution phase (steps 6–7) is Claude delivering against the accepted plan and the user reviewing against both the plan and the confirmed motivation. All deviations are user errors — something in Phase 0 or steps 1–5 that was missing, skipped, or accepted without verification. The only productive response to a gap between expected and actual output is to identify which precondition failed and change the user's behavior at that step. Full detail in `idealized-workflow.md`.
