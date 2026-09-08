import re

import pytest

import app.db.nebula_queries as nbq
from app.db.nebula_queries import (
    _resolve_system_ancestor_sync,
    resolve_system_ancestor,
)


class _Result:
    def __init__(self, rows, succeeded=True):
        self._rows = rows
        self._succeeded = succeeded

    def is_succeeded(self):
        return self._succeeded

    def row_size(self):
        return len(self._rows)

    def row_values(self, idx):
        return self._rows[idx]


def _vertex_string(id_, label):
    return f'("{id_}" :{label}' + '{name:"x"})'


def _label_for(id_):
    if id_.startswith("S"):
        return "SYSTEM"
    if id_.startswith("M"):
        return "MODULE"
    return "COMPONENT"


def _fake_session(edges):
    """edges: list of (from_id, to_id) representing HIERARCHY child->parent."""
    edge_map = {frm: to for frm, to in edges}

    class Session:
        def execute(self, query):
            query = query.strip()
            if query.startswith("USE"):
                return _Result([])
            if "FETCH PROP ON *" in query:
                m = re.search(r'"([^"]+)"', query)
                vid = m.group(1)
                # row is a single column: the vertex string
                return _Result([[ _vertex_string(vid, _label_for(vid)) ]])
            if "GO FROM" in query and "OVER HIERARCHY" in query:
                m = re.search(r'GO FROM "([^"]+)"', query)
                frm = m.group(1)
                parent = edge_map.get(frm)
                if parent is None:
                    return _Result([])
                # two columns: id($$) AS parent_id, $$ AS parent_vertex
                return _Result([[parent, _vertex_string(parent, _label_for(parent))]])
            return _Result([])

    return Session()


# --------------------------------------------------------------------------- #
# _resolve_system_ancestor_sync (sync core)
# --------------------------------------------------------------------------- #


def test_resolves_direct_module_to_system(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = _fake_session([("M1", "S1")])
    assert _resolve_system_ancestor_sync(session, "M1") == "S1"


def test_resolves_component_to_system(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = _fake_session([("C1", "M1"), ("M1", "S1")])
    assert _resolve_system_ancestor_sync(session, "C1") == "S1"


def test_returns_none_when_no_parent(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = _fake_session([])
    assert _resolve_system_ancestor_sync(session, "M1") is None


def test_stops_when_node_is_system(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = _fake_session([])
    assert _resolve_system_ancestor_sync(session, "S1") == "S1"


def test_cycle_detection_returns_none(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = _fake_session([("M1", "M2"), ("M2", "M1")])
    assert _resolve_system_ancestor_sync(session, "M1") is None


def test_empty_input_returns_none(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = _fake_session([])
    assert _resolve_system_ancestor_sync(session, "") is None


# --------------------------------------------------------------------------- #
# async resolve_system_ancestor (via _with_healthy_session)
# --------------------------------------------------------------------------- #


async def test_async_resolves_system_ancestor(monkeypatch):
    async def fake_with_healthy(coro_factory, *, retries=None):
        session = _fake_session([("C1", "M1"), ("M1", "S1")])
        obj = await coro_factory(session)
        return obj

    monkeypatch.setattr(nbq, "_with_healthy_session", fake_with_healthy)
    result = await resolve_system_ancestor("C1")
    assert result == "S1"


async def test_async_returns_none_on_empty(monkeypatch):
    async def fake_with_healthy(coro_factory, *, retries=None):
        session = _fake_session([])
        obj = await coro_factory(session)
        return obj

    monkeypatch.setattr(nbq, "_with_healthy_session", fake_with_healthy)
    assert await resolve_system_ancestor("") is None