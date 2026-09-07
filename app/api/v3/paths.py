import logging
from fastapi import APIRouter, HTTPException

from app.db.nebula_queries import (
    execute_nebula_experiment_search,
    fetch_nebula_node_names,
    fetch_one_hop_neighbors,
    fetch_one_hop_neighbors_to_finish,
)
from app.schemas.paths import (
    PathRequest,
    PathResponse,
    PathGroup,
    PathSegment,
    PathSegmentSource,
    PathSegmentDestination,
    SourceType,
    TraverseFilter,
    TraverseSortBy,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v3", tags=["paths"])


@router.post(
    "/paths",
    response_model=PathResponse,
    responses={
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
    openapi_extra={
        "x-mcp-tool-name": "build_path",
        "x-mcp-tool-description": "Поиск путей интеграции. Возвращает отрезки source -> destination.",
    },
)
async def path_search(request: PathRequest) -> PathResponse:
    start = request.start or TraverseFilter()
    finish = request.finish or TraverseFilter()

    if not finish.system_rsm_id and start.system_rsm_id:
        try:
            results = await fetch_one_hop_neighbors(
                start_filter=start,
                depth_days=request.depth_days,
                source_type=request.source.value,
            )
        except Exception as e:
            logger.error(f"NebulaGraph one-hop search error: {e}")
            raise HTTPException(
                status_code=500,
                detail={"code": "NEBULA_ERROR", "message": str(e)},
            )
    elif start.system_rsm_id and finish.system_rsm_id:
        try:
            results = await execute_nebula_experiment_search(
                start_filter=start,
                finish_filter=finish,
                depth_days=request.depth_days,
                source_type=request.source.value,
            )
        except Exception as e:
            logger.error(f"NebulaGraph search error: {e}")
            raise HTTPException(
                status_code=500,
                detail={"code": "NEBULA_ERROR", "message": str(e)},
            )
    elif finish.system_rsm_id and not start.system_rsm_id:
        try:
            results = await fetch_one_hop_neighbors_to_finish(
                finish_filter=finish,
                depth_days=request.depth_days,
                source_type=request.source.value,
            )
        except Exception as e:
            logger.error(f"NebulaGraph finish-anchored one-hop search error: {e}")
            raise HTTPException(
                status_code=500,
                detail={"code": "NEBULA_ERROR", "message": str(e)},
            )
    else:
        results = {}



    def to_combos(raw):
        """Normalize an edge data entry to a list of (module+component) combinations."""
        if isinstance(raw, dict):
            return [raw]
        return list(raw) if isinstance(raw, (list, tuple)) else []

    def eff_edge(nodes, i, is_reverse, combo):
        """Resolve the effective source/destination (sys, mod, comp) for an edge.

        Returns ((src_sys, src_mod, src_comp), (dst_sys, dst_mod, dst_comp)).
        """
        if is_reverse:
            src_node = nodes[i + 1]
            dst_node = nodes[i]
        else:
            src_node = nodes[i]
            dst_node = nodes[i + 1]

        src_mod = combo.get("consumer_module_id", "")
        src_comp = combo.get("consumer_component_id", "")
        dst_mod = combo.get("provider_module_id", "")
        dst_comp = combo.get("provider_component_id", "")

        if i == 0:
            if start.module_rsm_id:
                src_mod = start.module_rsm_id
            if start.component_rsm_id:
                src_comp = start.component_rsm_id
        if i == len(nodes) - 2:
            if finish.module_rsm_id:
                dst_mod = finish.module_rsm_id
            if finish.component_rsm_id:
                dst_comp = finish.component_rsm_id

        return (src_node, src_mod, src_comp), (dst_node, dst_mod, dst_comp)

    # Group by the full combination of (system, module, component) on every edge.
    # key1 = ((src_sys,src_mod,src_comp),(dst_sys,dst_mod,dst_comp), ...) per edge.
    combo_group_map: dict[tuple, dict] = {}
    all_nodes_set: set[tuple[str, str, str]] = set()

    for eotar_id, result_data in results.items():
        clean_eotar_id = eotar_id.strip('"')
        path_date = result_data.get("document_rsm_date_time")

        for _, data in result_data["paths"].items():
            path_nodes = data["path"]
            edge_data_list = data.get("edge_data", [])
            edge_directions = data.get("edge_directions", [])
            num_nodes = len(path_nodes)

            # Enumerate every combination of edge combos (cartesian product).
            edge_combo_lists = []
            for i in range(num_nodes - 1):
                is_reverse = i < len(edge_directions) and edge_directions[i] == "reverse"
                combos = to_combos(edge_data_list[i]) if i < len(edge_data_list) else []
                if not combos:
                    combos = [{
                        "consumer_module_id": "",
                        "provider_module_id": "",
                        "consumer_component_id": "",
                        "provider_component_id": "",
                    }]
                resolved = []
                for combo in combos:
                    src, dst = eff_edge(tuple(path_nodes), i, is_reverse, combo)
                    resolved.append((src, dst))
                edge_combo_lists.append(resolved)

            # Build the group key as the tuple of per-edge (src,dst) resolutions.
            # Combine the Cartesian product into distinct combination sequences.
            def cartesian_seq(lists):
                if not lists:
                    return [()]
                result = [[]]
                for edge_opts in lists:
                    new_result = []
                    for prefix in result:
                        for opt in edge_opts:
                            new_result.append(prefix + [opt])
                    result = new_result
                return [tuple(r) for r in result]

            for combo_seq in cartesian_seq(edge_combo_lists):
                group_key = (tuple(path_nodes), combo_seq)
                if group_key not in combo_group_map:
                    combo_group_map[group_key] = {
                        "nodes": tuple(path_nodes),
                        "edge_combo_seq": combo_seq,
                        "eotar_ids": set(),
                        "document_rsm_date_time": None,
                    }
                combo_group_map[group_key]["eotar_ids"].add(clean_eotar_id)
                existing_date = combo_group_map[group_key]["document_rsm_date_time"]
                if path_date and (not existing_date or path_date > existing_date):
                    combo_group_map[group_key]["document_rsm_date_time"] = path_date

                # Register name-lookup combos.
                for (src_sys, src_mod, src_comp), (dst_sys, dst_mod, dst_comp) in combo_seq:
                    all_nodes_set.add((src_sys, src_mod, src_comp))
                    all_nodes_set.add((dst_sys, dst_mod, dst_comp))
                    all_nodes_set.add((src_sys, "", ""))
                    all_nodes_set.add((dst_sys, "", ""))

    # Also add the finish system node names if finish filter is provided
    if finish.system_rsm_id:
        all_nodes_set.add((finish.system_rsm_id, "", ""))
    if start.system_rsm_id:
        all_nodes_set.add((start.system_rsm_id, "", ""))

    all_nodes = list(all_nodes_set)

    node_names = await fetch_nebula_node_names(all_nodes)


    path_groups: dict[tuple, dict] = {}

    # Pure in-memory assembly; no DB interaction needed (all node data is
    # already available in node_names / combo_group_map).
    def _build_path_groups() -> dict[tuple, dict]:
        for group_key, group in combo_group_map.items():
            path_nodes = group["nodes"]
            combo_seq = group["edge_combo_seq"]
            eotar_ids = group["eotar_ids"]

            segments: list[PathSegment] = []
            logger.debug(f"Building segments for key={group_key}, nodes={path_nodes}")
            for i in range(len(path_nodes) - 1):
                (src_sys, src_mod, src_comp), (dst_sys, dst_mod, dst_comp) = combo_seq[i]

                logger.debug(
                    f"  Segment {i}: "
                    f"source={src_sys}(mod={src_mod},comp={src_comp}) "
                    f"-> dest={dst_sys}(mod={dst_mod},comp={dst_comp})"
                )

                source_names = node_names.get((src_sys, src_mod, src_comp))
                source_system_names = node_names.get((src_sys, "", ""))
                dest_names = node_names.get((dst_sys, dst_mod, dst_comp))
                dest_system_names = node_names.get((dst_sys, "", ""))

                segments.append(PathSegment(
                    description="",
                    source=PathSegmentSource(
                        system_rsm_id=src_sys,
                        system_rsm_name=source_system_names.get("system_rsm_name") if source_system_names else None,
                        module_rsm_id=src_mod,
                        module_rsm_name=source_names.get("module_rsm_name") if source_names else None,
                        component_rsm_id=src_comp,
                        component_rsm_name=source_names.get("component_rsm_name") if source_names else None,
                    ),
                    destination=PathSegmentDestination(
                        system_rsm_id=dst_sys,
                        system_rsm_name=dest_system_names.get("system_rsm_name") if dest_system_names else None,
                        module_rsm_id=dst_mod,
                        module_rsm_name=dest_names.get("module_rsm_name") if dest_names else None,
                        component_rsm_id=dst_comp,
                        component_rsm_name=dest_names.get("component_rsm_name") if dest_names else None,
                    ),
                ))

            sorted_eotar_ids = sorted(eotar_ids)
            first_eotar_id = sorted_eotar_ids[0] if sorted_eotar_ids else ""

            path_groups[group_key] = {
                "segments": segments,
                "frequency": len(eotar_ids),
                "document_rsm_id": first_eotar_id,
                "document_rsm_date_time": group.get("document_rsm_date_time"),
            }
        return path_groups

    path_groups = _build_path_groups()

    if request.sort_by == TraverseSortBy.MOST_FREQUENT:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: -x["frequency"]
        )
    elif request.sort_by == TraverseSortBy.LONGEST:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (-len(x["segments"]), -x["frequency"])
        )
    elif request.sort_by == TraverseSortBy.SHORTEST:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (len(x["segments"]), -x["frequency"])
        )
    elif request.sort_by == TraverseSortBy.MOST_RECENT:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (x.get("document_rsm_date_time") is None, x.get("document_rsm_date_time") or "", -x["frequency"]),
            reverse=True
        )
    else:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: -x["frequency"]
        )

    result = []
    for p in sorted_paths[:request.path_count]:
        parts = []
        freq = p.get("frequency", 0)
        doc_id = p.get("document_rsm_id")
        doc_date = p.get("document_rsm_date_time")
        parts.append(f"frequency: {freq}")
        if doc_id:
            parts.append(f"document_rsm_id: {doc_id}")
        if doc_date:
            parts.append(f"document_rsm_date_time: {doc_date}")
        result.append(PathGroup(
            segments=p["segments"],
            description=" | ".join(parts),
        ))
    return PathResponse(paths=result)