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

    async def fake_search(start_filter, finish_filter, depth_days, source_type, max_path_depth=7, path_limit=100):
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


async def test_path_search_swaps_module_component_on_reverse_edge(monkeypatch, mocker):
    import app.api.v3.paths as paths_mod

    # Simulate the graph edge directional semantics:
    #   provider is the endpoint of the edge in its stored direction,
    #   consumer is the start of the edge in its stored direction.
    # A "reverse" traversal means we walk consumer<-provider, so:
    #   source (nodes[i+1]) belongs to provider_*, destination (nodes[i]) to consumer_*.
    results = {
        '"DOC1"': {
            "document_rsm_date_time": "2026-01-01T00:00:00",
            "paths": {
                ("S_DST", "S_RES"): {
                    # Traversed against the stored edge direction: consumer<-provider.
                    # nodes[0]=S_DST (edge endpoint/provider), nodes[1]=S_RES (edge start/consumer).
                    "path": ["S_DST", "S_RES"],
                    "distance": 2,
                    "edge_data": [
                        {
                            "consumer_module_id": "MOD_CONSUMER",
                            "provider_module_id": "MOD_PROVIDER",
                            "consumer_component_id": "CMP_CONSUMER",
                            "provider_component_id": "CMP_PROVIDER",
                        }
                    ],
                    "edge_directions": ["reverse"],
                }
            },
        }
    }

    async def fake_search(start_filter, finish_filter, depth_days, source_type, max_path_depth=7, path_limit=100):
        return results

    async def fake_names(nodes):
        return {
            ("S_RES", "MOD_PROVIDER", "CMP_PROVIDER"): {
                "system_rsm_name": "RES",
                "module_rsm_name": "MOD_PROVIDER",
                "component_rsm_name": "CMP_PROVIDER",
            },
            ("S_DST", "MOD_CONSUMER", "CMP_CONSUMER"): {
                "system_rsm_name": "DST",
                "module_rsm_name": "MOD_CONSUMER",
                "component_rsm_name": "CMP_CONSUMER",
            },
        }

    monkeypatch.setattr(paths_mod, "execute_nebula_experiment_search", fake_search)
    monkeypatch.setattr(paths_mod, "fetch_nebula_node_names", fake_names)

    req = PathRequest(
        start={"system_rsm_id": "S_RES"},
        finish={"system_rsm_id": "S_DST"},
    )
    resp = await path_search(req)

    assert len(resp.paths) == 1
    seg = resp.paths[0].segments[0]
    # Reverse edge: source takes provider_*, destination takes consumer_*.
    assert seg.source.system_rsm_id == "S_RES"
    assert seg.source.module_rsm_id == "MOD_PROVIDER"
    assert seg.source.component_rsm_id == "CMP_PROVIDER"
    assert seg.destination.system_rsm_id == "S_DST"
    assert seg.destination.module_rsm_id == "MOD_CONSUMER"
    assert seg.destination.component_rsm_id == "CMP_CONSUMER"


async def test_path_search_keeps_consumer_provider_on_forward_edge(monkeypatch, mocker):
    import app.api.v3.paths as paths_mod

    results = {
        '"DOC1"': {
            "document_rsm_date_time": "2026-01-01T00:00:00",
            "paths": {
                ("S_RES", "S_DST"): {
                    "path": ["S_RES", "S_DST"],
                    "distance": 2,
                    "edge_data": [
                        {
                            "consumer_module_id": "MOD_CONSUMER",
                            "provider_module_id": "MOD_PROVIDER",
                            "consumer_component_id": "CMP_CONSUMER",
                            "provider_component_id": "CMP_PROVIDER",
                        }
                    ],
                    "edge_directions": ["forward"],
                }
            },
        }
    }

    async def fake_search(start_filter, finish_filter, depth_days, source_type, max_path_depth=7, path_limit=100):
        return results

    async def fake_names(nodes):
        return {
            ("S_RES", "MOD_CONSUMER", "CMP_CONSUMER"): {
                "system_rsm_name": "RES",
                "module_rsm_name": "MOD_CONSUMER",
                "component_rsm_name": "CMP_CONSUMER",
            },
            ("S_DST", "MOD_PROVIDER", "CMP_PROVIDER"): {
                "system_rsm_name": "DST",
                "module_rsm_name": "MOD_PROVIDER",
                "component_rsm_name": "CMP_PROVIDER",
            },
        }

    monkeypatch.setattr(paths_mod, "execute_nebula_experiment_search", fake_search)
    monkeypatch.setattr(paths_mod, "fetch_nebula_node_names", fake_names)

    req = PathRequest(
        start={"system_rsm_id": "S_RES"},
        finish={"system_rsm_id": "S_DST"},
    )
    resp = await path_search(req)

    assert len(resp.paths) == 1
    seg = resp.paths[0].segments[0]
    # Forward edge: source keeps consumer_*, destination keeps provider_*.
    assert seg.source.module_rsm_id == "MOD_CONSUMER"
    assert seg.source.component_rsm_id == "CMP_CONSUMER"
    assert seg.destination.module_rsm_id == "MOD_PROVIDER"
    assert seg.destination.component_rsm_id == "CMP_PROVIDER"