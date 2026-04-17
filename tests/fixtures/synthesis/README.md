# tests/fixtures/synthesis

Golden SQLite fixture for Epic #17 (Weekly Synthesis Engine).

`golden.sqlite` is a committed binary artifact. It is consumed by
synthesis-pipeline tests across Sub-Issues 2–7 so each test loads a small
self-contained DB instead of stubbing collector readers.

## Topology

Three units, all anchored to `week_start = 2025-01-06` (Monday) — matching
the `SynthesisConfig.week_start = "monday"` default.

| unit_id                | Shape                                | Purpose                                                                                    |
|------------------------|--------------------------------------|--------------------------------------------------------------------------------------------|
| `unit-0001-multi`      | 2 sessions, 2 merged PRs, 1 issue   | Multi-session / multi-PR unit. Exercises `pr_sessions` joins and cross-session `dark_time_pct`. |
| `unit-0002-abandoned`  | 1 open issue, 1 closed-unmerged PR, no recent sessions | Abandonment flag. Last activity 2024-12-12 — older than `abandonment_days=14` default.      |
| `unit-0003-singleton`  | 1 session, no PR, no issue          | Singleton. `dark_time_pct` must be 0.0 for single-session units.                            |

Additional rows populate `commits`, `timeline_events`, `graph_nodes`, and
`graph_edges` so JOINs across the synthesis tables have something to land
against.

## Regenerating

From the repo root:

```bash
python tests/fixtures/synthesis/build_golden.py
git add tests/fixtures/synthesis/golden.sqlite
```

`build_golden.py` is deterministic: running it twice yields a byte-identical
file (fixed UUIDs, fixed timestamps, fixed insert order, `PRAGMA
journal_mode=DELETE` to keep the `.wal` sidecar from landing on disk). The
schema is always sourced from `am_i_shipping.db.init_github_db` so a change
to `db.py` automatically propagates to the next fixture rebuild.

## Consumers

| Test                                                   | Why it reads the fixture                                   |
|--------------------------------------------------------|------------------------------------------------------------|
| `tests/test_init_db.py::test_golden_fixture_loadable`  | Confirms the fixture is committed and has 3 unit_ids.      |

Sub-issues 2–7 will add more consumers (unit identifier, metrics, prompt
assembly, output writer). When they do, extend this table.
