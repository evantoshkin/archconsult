import re

import pytest

import app.db.nebula_queries as nbq
from app.db.nebula_queries import _fetch_nebula_node_names_sync


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

    def error_msg(self):
        return "mock failure" if not self._succeeded else ""


def _vertex(id_, tag, name="x"):
    return f'("{id_}" :{tag}' + f'{{name:"{name}"}})'.replace("}})}}", "}}")


class NameSession:
    """Captures FETCH PROP ON <tag> queries; returns a name per id."""

    def __init__(self):
        self.queries = []

    def execute(self, query):
        q = query.strip()
        self.queries.append(q)
        if q.startswith("USE"):
            return _Result([])
        if "FETCH PROP ON " in q:
            # e.g. FETCH PROP ON MODULE "M1","M2" YIELD vertex as v
            ids_part = q.split("ON", 1)[1].split("YIELD", 1)[0]
            # forms: MODULE "M1","M2"  OR  SYSTEM "S1","S2" (tag with no space? tag always separate)
            tag = ids_part.split()[0].strip()
            raw_vids = re.findall(r'"([^"]+)"', ids_part)
            rows = []
            for vid in raw_vids:
                if tag == "MODULE":
                    rows.append([_vertex(vid, "MODULE", f"mod-{vid}")])
                elif tag == "COMPONENT":
                    rows.append([_vertex(vid, "COMPONENT", f"comp-{vid}")])
                else:
                    rows.append([_vertex(vid, "SYSTEM", f"sys-{vid}")])
            return _Result(rows)
        return _Result([])


def test_name_fetch_batches_module_and_component(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")
    session = NameSession()

    nodes = [
        ("S1", "M1", "C1"),
        ("S1", "M2", "C2"),
    ]
    names = _fetch_nebula_node_names_sync(session, nodes)

    # Exactly ONE FETCH PROP ON MODULE (batched) and ONE for COMPONENT.
    module_qs = [q for q in session.queries if "FETCH PROP ON MODULE" in q]
    comp_qs = [q for q in session.queries if "FETCH PROP ON COMPONENT" in q]
    assert len(module_qs) == 1, f"expected batched MODULE query, got {len(module_qs)}: {module_qs}"
    assert len(comp_qs) == 1, f"expected batched COMPONENT query, got {len(comp_qs)}: {comp_qs}"

    # The single batched query carries both ids.
    assert '"M1"' in module_qs[0] and '"M2"' in module_qs[0]
    assert '"C1"' in comp_qs[0] and '"C2"' in comp_qs[0]

    # Names resolved into the right (sys, mod, comp) keys.
    assert names[("S1", "M1", "C1")]["module_rsm_name"] == "mod-M1"
    assert names[("S1", "M1", "C1")]["component_rsm_name"] == "comp-C1"
    assert names[("S1", "M2", "C2")]["module_rsm_name"] == "mod-M2"
    assert names[("S1", "M2", "C2")]["component_rsm_name"] == "comp-C2"


def test_name_fetch_per_id_fallback_on_batch_failure(monkeypatch):
    monkeypatch.setattr(nbq.settings, "NEBULA_SPACE", "test_space")

    class FailingBatchSession(NameSession):
        def execute(self, query):
            q = query.strip()
            self.queries.append(q)
            if q.startswith("USE"):
                return _Result([])
            if "FETCH PROP ON " in q:
                ids_part = q.split("ON", 1)[1].split("YIELD", 1)[0]
                tag = ids_part.split()[0].strip()
                raw_vids = re.findall(r'"([^"]+)"', ids_part)
                # Multi-id batch rejected -> fail so per-id fallback kicks in.
                if len(raw_vids) > 1:
                    return _Result([], succeeded=False)
                vid = raw_vids[0]
                if tag == "MODULE":
                    return _Result([[_vertex(vid, "MODULE", f"mod-{vid}")]])
                return _Result([[_vertex(vid, "COMPONENT", f"comp-{vid}")]])
            return _Result([])

    session = FailingBatchSession()
    nodes = [("S1", "M1", "C1"), ("S1", "M2", "C2")]
    names = _fetch_nebula_node_names_sync(session, nodes)

    # Fallback: one single-id query per id (2 MODULE + 2 COMPONENT single).
    single_mod = [q for q in session.queries if "FETCH PROP ON MODULE" in q and '"M1","M2"' not in q]
    single_comp = [q for q in session.queries if "FETCH PROP ON COMPONENT" in q and '"C1","C2"' not in q]
    assert len(single_mod) == 2, f"expected 2 single MODULE fetches, got {len(single_mod)}: {single_mod}"
    assert len(single_comp) == 2, f"expected 2 single COMPONENT fetches, got {len(single_comp)}: {single_comp}"
    assert names[("S1", "M1", "C1")]["module_rsm_name"] == "mod-M1"
    assert names[("S1", "M2", "C2")]["component_rsm_name"] == "comp-C2"