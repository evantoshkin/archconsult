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


class BatchingSession:
    """Records queries; returns a FIND PATH result for one doc and empty edges."""

    def __init__(self, path_rows=None):
        self.queries = []
        self._path_rows = path_rows or [
            ['("S1":SYSTEM{name:"x"})<-("M1":MODULE{name:"x"})->("S2":SYSTEM{name:"x"})'],
        ]

    def execute(self, query):
        q = query.strip()
        self.queries.append(q)
        if "FETCH PROP ON SYSTEM" in q:
            return _Result([['("S1" :SYSTEM{system_rsm_id:"S1",name:"x"})']])
        if "FETCH PROP ON *" in q:
            m = re.search(r'"([^"]+)"', q)
            vid = m.group(1)
            label = "SYSTEM" if vid.endswith("S") else ("MODULE" if vid.endswith("M") else "COMPONENT")
            return _Result([(f'("{vid}" :{label}' + '{name:"x"})',)])
        if "GO FROM" in q and "OVER HIERARCHY" in q:
            return _Result([])
        if "FIND NOLOOP PATH" in q:
            return _Result(self._path_rows)
        if "GO FROM" in q and "BIDIRECT" in q and "src(edge)" not in q:
            # Outgoing/incoming document scan for the doc matching to proceed.
            # Column order: rsm_document_id, rsm_document_date, consumer_module,
            # provider_module, consumer_component, provider_component, rsm_diagram_id.
            return _Result([['"DOC1"', '"2026-01-01T12:00:00"', "", "", "", "", '"DIAG1"']])
        if "GO FROM" in q and ("src(edge)" in q or "YIELD src(edge)" in q):
            # Batched edge fetch -> return empty (no edge combos).
            return _Result([])
        return _Result([])


def test_edge_fetch_is_batched_per_document(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")

    # Force the FIND PATH to return a 3-node path, so only ONE edge exists.
    session = BatchingSession()
    start = TraverseFilter(system_rsm_id="S1")
    finish = TraverseFilter(system_rsm_id="S2")
    _execute_experiment_search_sync(session, start, finish, "2026-01-01T00:00:00", "VISION_INTERFACE_SYSTEM_LEVEL")

    edge_queries = [q for q in session.queries if "src(edge)" in q or "YIELD src(edge)" in q]
    # At most ONE forward batched query (and possibly one reverse). Never per-edge.
    assert len(edge_queries) >= 1, "expected at least one batched edge query"
    assert len(edge_queries) <= 2, f"expected <=2 batched edge queries, got {len(edge_queries)}"
    union = " ".join(edge_queries)
    assert "GO FROM" in union
    # The batched query must project source/destination to pair edges back up.
    assert "src(edge)" in union
    assert "dst(edge)" in union


def test_edge_fetch_uses_multi_source_in_clause(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")

    # Return a 4-node path -> 3 edges -> should produce a multi-source GO FROM.
    path = ['("S1":SYSTEM{name:"x"})<-("A":MODULE{name:"x"})<-("B":MODULE{name:"x"})->("S2":SYSTEM{name:"x"})']
    session = BatchingSession(path_rows=[path])
    start = TraverseFilter(system_rsm_id="S1")
    finish = TraverseFilter(system_rsm_id="S2")
    _execute_experiment_search_sync(session, start, finish, "2026-01-01T00:00:00", "VISION_INTERFACE_SYSTEM_LEVEL")

    edge_queries = [q for q in session.queries if "src(edge)" in q]
    assert edge_queries
    # The forward batched query should carry multiple source vids (comma-joined IN list).
    assert any('id($$)' in q for q in edge_queries)