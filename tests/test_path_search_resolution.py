import pytest

from app.api.v3.paths import path_search
from app.schemas.paths import PathRequest


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


def _vertex(id_, label):
    return (f'("{id_}" :{label}' + '{name:"x"})',)


def _fake_session(edges):
    edge_map = {frm: to for frm, to in edges}

    class Session:
        def execute(self, query):
            import re

            query = query.strip()
            if query.startswith("USE"):
                return _Result([])
            if "FETCH PROP ON *" in query:
                m = re.search(r'"([^"]+)"', query)
                vid = m.group(1)
                label = "SYSTEM" if vid.endswith("S") else ("MODULE" if vid.endswith("M") else "COMPONENT")
                return _Result(_vertex(vid, label))
            if "GO FROM" in query and "OVER HIERARCHY" in query:
                m = re.search(r'GO FROM "([^"]+)"', query)
                frm = m.group(1)
                parent = edge_map.get(frm)
                if parent is None:
                    return _Result([])
                label = "SYSTEM" if parent.endswith("S") else ("MODULE" if parent.endswith("M") else "COMPONENT")
                return _Result(_vertex(parent, label))
            return _Result([])

    return Session()


# --------------------------------------------------------------------------- #
# path_search: system resolution for start / finish
# --------------------------------------------------------------------------- #


async def test_path_search_resolves_start_module_to_system(tmp_path, monkeypatch, mocker):
    import app.api.v3.paths as paths_mod

    captured = {}

    async def fake_resolve(anchor):
        captured["anchor"] = anchor
        return "S1"

    async def fake_one_hop(start_filter, depth_days, source_type):
        captured["filters"] = start_filter
        return {}

    async def fake_names(nodes):
        return {}

    monkeypatch.setattr(paths_mod, "resolve_system_ancestor", fake_resolve)
    monkeypatch.setattr(paths_mod, "fetch_one_hop_neighbors", fake_one_hop)
    monkeypatch.setattr(paths_mod, "fetch_nebula_node_names", fake_names)

    req = PathRequest(start={"module_rsm_id": "M1"})
    await path_search(req)

    assert captured.get("anchor") == "M1"
    assert captured["filters"].system_rsm_id == "S1"


async def test_path_search_resolves_finish_module_to_system(tmp_path, monkeypatch, mocker):
    import app.api.v3.paths as paths_mod

    captured = {}

    async def fake_resolve(anchor):
        captured["anchor"] = anchor
        return "S2"

    async def fake_to_finish(finish_filter, depth_days, source_type):
        captured["filters"] = finish_filter
        return {}

    async def fake_names(nodes):
        return {}

    monkeypatch.setattr(paths_mod, "resolve_system_ancestor", fake_resolve)
    monkeypatch.setattr(paths_mod, "fetch_one_hop_neighbors_to_finish", fake_to_finish)
    monkeypatch.setattr(paths_mod, "fetch_nebula_node_names", fake_names)

    req = PathRequest(finish={"module_rsm_id": "M2"})
    await path_search(req)

    assert captured.get("anchor") == "M2"
    assert captured["filters"].system_rsm_id == "S2"


async def test_path_search_resolves_both_when_no_system(tmp_path, monkeypatch, mocker):
    import app.api.v3.paths as paths_mod

    anchors = []
    captured = {}

    async def fake_resolve(anchor):
        anchors.append(anchor)
        return f"SYS-{anchor}"

    async def fake_search(start_filter, finish_filter, depth_days, source_type):
        captured["start"] = start_filter.system_rsm_id
        captured["finish"] = finish_filter.system_rsm_id
        return {}

    async def fake_names(nodes):
        return {}

    monkeypatch.setattr(paths_mod, "resolve_system_ancestor", fake_resolve)
    monkeypatch.setattr(paths_mod, "execute_nebula_experiment_search", fake_search)
    monkeypatch.setattr(paths_mod, "fetch_nebula_node_names", fake_names)

    req = PathRequest(start={"module_rsm_id": "M1"}, finish={"component_rsm_id": "C2"})
    await path_search(req)

    assert anchors == ["M1", "C2"]
    assert captured["start"] == "SYS-M1"
    assert captured["finish"] == "SYS-C2"


async def test_path_search_leaves_system_empty_when_no_ancestor(tmp_path, monkeypatch, mocker):
    import app.api.v3.paths as paths_mod

    async def fake_resolve(anchor):
        return None

    async def fake_names(nodes):
        return {}

    # No branch: both start & finish have no system after resolve -> results={}
    monkeypatch.setattr(paths_mod, "resolve_system_ancestor", fake_resolve)
    monkeypatch.setattr(paths_mod, "fetch_nebula_node_names", fake_names)

    req = PathRequest(start={"module_rsm_id": "M1"}, finish={"module_rsm_id": "M2"})
    resp = await path_search(req)
    assert resp.paths == []