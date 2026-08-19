import logging
from fastapi import APIRouter, HTTPException

from app.core.config import settings
from app.db.nebula_pool import get_nebula_pool
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



    path_eotar_map: dict[tuple, set[str]] = {}
    path_data_map: dict[tuple, dict] = {}

    for eotar_id, result_data in results.items():
        clean_eotar_id = eotar_id.strip('"')

        for path_key, data in result_data["paths"].items():
            if path_key not in path_eotar_map:
                path_eotar_map[path_key] = set()
                path_data_map[path_key] = {
                    "path": data["path"],
                    "edge_data": data.get("edge_data", []),
                    "edge_directions": data.get("edge_directions", []),
                    "document_rsm_date_time": result_data.get("document_rsm_date_time"),
                }
            path_eotar_map[path_key].add(clean_eotar_id)

            existing_date = path_data_map[path_key].get("document_rsm_date_time")
            new_date = result_data.get("document_rsm_date_time")
            if new_date and (not existing_date or new_date > existing_date):
                path_data_map[path_key]["document_rsm_date_time"] = new_date
    # Collect all (node_id, module_id, component_id) combinations needed for name lookups.
    # Must run after path_data_map is populated.
    def to_combos(raw):
        """Normalize an edge data entry to a list of (module+component) combinations."""
        if isinstance(raw, dict):
            return [raw]
        return list(raw) if isinstance(raw, (list, tuple)) else []

    all_nodes_set: set[tuple[str, str, str]] = set()
    for path_key, data in path_data_map.items():
        path_nodes = data["path"]
        edge_data_list = data.get("edge_data", [])
        edge_directions = data.get("edge_directions", [])
        num_nodes = len(path_nodes)

        for i in range(num_nodes - 1):
            source_node = path_nodes[i]
            dest_node = path_nodes[i + 1]
            combos = to_combos(edge_data_list[i]) if i < len(edge_data_list) else []
            is_reverse = i < len(edge_directions) and edge_directions[i] == "reverse"

            if combos:
                if is_reverse:
                    # Edge opposite to path direction: nodes[i] <- nodes[i+1].
                    # source (consumer) = nodes[i+1], destination (provider) = nodes[i].
                    eff_source_node = path_nodes[i + 1]
                    eff_dest_node = path_nodes[i]
                else:
                    eff_source_node = path_nodes[i]
                    eff_dest_node = path_nodes[i + 1]
            else:
                eff_source_node = path_nodes[i]
                eff_dest_node = path_nodes[i + 1]

            for combo in combos:
                if is_reverse:
                    source_module_id = combo.get("consumer_module_id", "")
                    source_component_id = combo.get("consumer_component_id", "")
                    dest_module_id = combo.get("provider_module_id", "")
                    dest_component_id = combo.get("provider_component_id", "")
                else:
                    source_module_id = combo.get("consumer_module_id", "")
                    source_component_id = combo.get("consumer_component_id", "")
                    dest_module_id = combo.get("provider_module_id", "")
                    dest_component_id = combo.get("provider_component_id", "")

                all_nodes_set.add((eff_source_node, source_module_id, source_component_id))
                all_nodes_set.add((eff_dest_node, dest_module_id, dest_component_id))

            # Always add the bare system nodes for system-level name lookups.
            all_nodes_set.add((eff_source_node, "", ""))
            all_nodes_set.add((eff_dest_node, "", ""))

            # Also add overridden module/component IDs from request filters
            if i == 0 and start.module_rsm_id:
                all_nodes_set.add((eff_source_node, start.module_rsm_id, start.component_rsm_id))
            if i == num_nodes - 2 and finish.module_rsm_id:
                all_nodes_set.add((eff_dest_node, finish.module_rsm_id, finish.component_rsm_id))

    # Also add the finish system node names if finish filter is provided
    if finish.system_rsm_id:
        all_nodes_set.add((finish.system_rsm_id, "", ""))

    all_nodes = list(all_nodes_set)

    node_names = await fetch_nebula_node_names(all_nodes)


    path_groups: dict[tuple, dict] = {}

    pool = get_nebula_pool()
    session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)

    try:
        session.execute(f'USE {settings.NEBULA_SPACE};')

        for path_key, eotar_ids in path_eotar_map.items():
            data = path_data_map[path_key]
            path_nodes = data["path"]
            edge_data_list = data.get("edge_data", [])
            edge_directions = data.get("edge_directions", [])
            num_nodes = len(path_nodes)
            document_id = sorted(eotar_ids)[0] if eotar_ids else ""

            segments: list[PathSegment] = []
            logger.info(f"Building segments for path_key={path_key}, nodes={path_nodes}, edge_directions={edge_directions}")
            for i in range(num_nodes - 1):
                is_reverse = i < len(edge_directions) and edge_directions[i] == "reverse"

                if is_reverse:
                    # Edge opposite to path direction: nodes[i] <- nodes[i+1].
                    # source (consumer/requester) = nodes[i+1], destination (provider/responder) = nodes[i].
                    eff_source_node = path_nodes[i + 1]
                    eff_dest_node = path_nodes[i]
                else:
                    eff_source_node = path_nodes[i]
                    eff_dest_node = path_nodes[i + 1]

                combos = to_combos(edge_data_list[i]) if i < len(edge_data_list) else []
                if not combos:
                    combos = [{
                        "consumer_module_id": "",
                        "provider_module_id": "",
                        "consumer_component_id": "",
                        "provider_component_id": "",
                    }]

                for combo in combos:
                    source_module_id = combo.get("consumer_module_id", "")
                    source_component_id = combo.get("consumer_component_id", "")
                    dest_module_id = combo.get("provider_module_id", "")
                    dest_component_id = combo.get("provider_component_id", "")

                    if i == 0:
                        if start.module_rsm_id:
                            source_module_id = start.module_rsm_id
                        if start.component_rsm_id:
                            source_component_id = start.component_rsm_id

                    if i == num_nodes - 2:
                        if finish.module_rsm_id:
                            dest_module_id = finish.module_rsm_id
                        if finish.component_rsm_id:
                            dest_component_id = finish.component_rsm_id

                    logger.info(
                        f"  Segment {i} ({'REVERSE' if is_reverse else 'FORWARD'}): "
                        f"source={eff_source_node}(mod={source_module_id},comp={source_component_id}) "
                        f"-> dest={eff_dest_node}(mod={dest_module_id},comp={dest_component_id})"
                    )

                    source_names = node_names.get((eff_source_node, source_module_id, source_component_id))
                    source_system_names = node_names.get((eff_source_node, "", ""))
                    dest_names = node_names.get((eff_dest_node, dest_module_id, dest_component_id))
                    dest_system_names = node_names.get((eff_dest_node, "", ""))

                    segments.append(PathSegment(
                        description="",
                        source=PathSegmentSource(
                            system_rsm_id=eff_source_node,
                            system_rsm_name=source_system_names.get("system_rsm_name") if source_system_names else None,
                            module_rsm_id=source_module_id,
                            module_rsm_name=source_names.get("module_rsm_name") if source_names else None,
                            component_rsm_id=source_component_id,
                            component_rsm_name=source_names.get("component_rsm_name") if source_names else None,
                        ),
                        destination=PathSegmentDestination(
                            system_rsm_id=eff_dest_node,
                            system_rsm_name=dest_system_names.get("system_rsm_name") if dest_system_names else None,
                            module_rsm_id=dest_module_id,
                            module_rsm_name=dest_names.get("module_rsm_name") if dest_names else None,
                            component_rsm_id=dest_component_id,
                            component_rsm_name=dest_names.get("component_rsm_name") if dest_names else None,
                        ),
                    ))

            sorted_eotar_ids = sorted(eotar_ids)
            first_eotar_id = sorted_eotar_ids[0] if sorted_eotar_ids else ""

            path_groups[path_key] = {
                "segments": segments,
                "frequency": len(eotar_ids),
                "document_rsm_id": first_eotar_id,
                "document_rsm_date_time": data.get("document_rsm_date_time"),
            }
    finally:
        session.release()

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