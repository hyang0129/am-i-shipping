"""Shared test configuration.

Integration-gate Phase 2 tests (see
``tests/test_integration_gate.py::TestPhase2Pipeline``) assume the
live ``data/github.db`` does not contain stale rows keyed on the
sentinel ``week_start = 'all'`` value. That sentinel was written by a
pre-#55 smoke test and silently distorts idempotency assertions
because those rows are not keyed on a real ISO week.

The cleanup is performed per-test inside ``TestPhase2Pipeline`` via
``_clean_stale_phase2_rows``; it is documented here so anyone reading
``conftest.py`` first can find the pointer. If a future suite needs
the same behaviour at session scope, lift that helper into an
``autouse`` fixture here.

Manual cleanup (equivalent SQL, for reference):

    DELETE FROM graph_nodes WHERE week_start = 'all';
    DELETE FROM graph_edges WHERE week_start = 'all';
    DELETE FROM units       WHERE week_start = 'all';
"""

import pytest


@pytest.fixture(autouse=True)
def _force_offline_llm(monkeypatch: pytest.MonkeyPatch):
    """Force the offline ``_FakeAdapter`` for every test.

    Production defaults to live; tests must opt out so they never hit
    the network or burn API credit. A test that needs live behaviour
    can ``monkeypatch.delenv("AMIS_SYNTHESIS_OFFLINE", raising=False)``
    inside its own body.
    """
    monkeypatch.setenv("AMIS_SYNTHESIS_OFFLINE", "1")
