import re

import pytest

import app.db.nebula_queries as nbq
from app.db.nebula_queries import _execute_experiment_search_sync
from app.schemas.paths import TraverseFilter


class _Result:
    def __init__(self, rows=None, succeeded=True):
        self._rows = rows or []
        self._succeeded = succeeded

    def is_succeeded(self):
        return self._succeeded

    def row_size(self):
        return len(self._rows)

    def row_values(self, idx):
        return self._rows[idx]


class RecordingSession:
    """Captures every GO ... incoming query so we can assert on finish filters."""

    def __init__(self, start_sys="S1", finish_sys="S2"):
        self.start_sys = start_sys
        self.finish_sys = finish_sys
        self.queries = []

    def execute(self, query):
        self.queries.append(query.strip())
        q = query.strip()
        if "FETCH PROP ON SYSTEM" in q:
            # A single row so the start-system existence check passes.
            return _Result([[ '("S1" :SYSTEM{system_rsm_id:"S1",name:"x"})' ]])
        # Return empty results for the rest so the flow is simple.
        return _Result([])


def test_incoming_query_applies_finish_module_and_component_filters(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")

    start = TraverseFilter(system_rsm_id="S1", module_rsm_id="M_START")
    finish = TraverseFilter(system_rsm_id="S2", module_rsm_id="M_FINISH", component_rsm_id="C_FINISH")

    session = RecordingSession(start_sys="S1", finish_sys="S2")
    _execute_experiment_search_sync(session, start, finish, "2026-01-01T00:00:00", "VISION_INTERFACE_SYSTEM_LEVEL")

    # Find the incoming query built from finish system.
    incoming = [q for q in session.queries if f'GO FROM "{session.finish_sys}"' in q]
    assert incoming, "expected an incoming GO FROM query for finish system"
    inc = incoming[0]
    assert f'provider_module_id == "{finish.module_rsm_id}"' in inc
    assert f'provider_component_id == "{finish.component_rsm_id}"' in inc


def test_incoming_query_has_no_module_filter_when_not_provided(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")

    start = TraverseFilter(system_rsm_id="S1")
    finish = TraverseFilter(system_rsm_id="S2")

    session = RecordingSession(start_sys="S1", finish_sys="S2")
    _execute_experiment_search_sync(session, start, finish, "2026-01-01T00:00:00", "VISION_INTERFACE_SYSTEM_LEVEL")

    incoming = [q for q in session.queries if f'GO FROM "{session.finish_sys}"' in q]
    assert incoming
    inc = incoming[0]
    assert "provider_module_id ==" not in inc
    assert "provider_component_id ==" not in inc