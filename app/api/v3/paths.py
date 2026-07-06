import logging
from fastapi import APIRouter, HTTPException

from app.core.config import settings
from app.db.nebula_pool import get_nebula_pool
from app.db.nebula_queries import execute_nebula_experiment_search, fetch_nebula_node_names
from app.schemas.paths import (
    PathRequest,
    PathResponse,
    PathGroup,
    PathSegment,
    PathSegmentSource,
    PathSegmentDestination,
    SourceType,
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
    try:
        results = await execute_nebula_experiment_search(
            start_filter=request.start,
            finish_filter=request.finish,
            depth_days=request.depth_days,
            source_type=request.source.value,
        )
    except Exception as e:
        logger.error(f"NebulaGraph search error: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "NEBULA_ERROR", "message": str(e)},
        )

    all_nodes: list[tuple[str, str, str]] = []
    for eotar_id, data in results.items():
        for path_key, path_data in data["paths"].items():
            edge_data_list = path_data.get("edge_data", [])
            nodes = path_data["path"]
            num_nodes = len(nodes)

            for i, node_id in enumerate(nodes):
                module_id = ""
                component_id = ""

                if i == 0 and num_nodes > 1 and len(edge_data_list) > 0:
                    edge = edge_data_list[0]
                    module_id = edge.get("consumer_module_id", "")
                    component_id = edge.get("consumer_component_id", "")
                elif i == num_nodes - 1 and num_nodes > 1 and len(edge_data_list) >= num_nodes - 1:
                    edge = edge_data_list[num_nodes - 2]
                    module_id = edge.get("provider_module_id", "")
                    component_id = edge.get("provider_component_id", "")
                elif i > 0 and i < num_nodes - 1:
                    if len(edge_data_list) >= i:
                        edge = edge_data_list[i - 1]
                        module_id = edge.get("provider_module_id", "")
                        component_id = edge.get("provider_component_id", "")

                all_nodes.append((node_id, module_id, component_id))

    node_names = await fetch_nebula_node_names(all_nodes)

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
            for i in range(num_nodes - 1):
                source_node = path_nodes[i]
                dest_node = path_nodes[i + 1]

                source_module_id = ""
                source_component_id = ""
                dest_module_id = ""
                dest_component_id = ""

                if i < len(edge_data_list):
                    edge = edge_data_list[i]
                    is_reverse = i < len(edge_directions) and edge_directions[i] == "reverse"

                    if is_reverse:
                        # Edge goes opposite to path direction
                        # source (requester) = consumer = nodes[i+1]
                        # destination = provider = nodes[i]
                        source_node = path_nodes[i + 1]
                        dest_node = path_nodes[i]
                        source_module_id = edge.get("provider_module_id", "")
                        source_component_id = edge.get("provider_component_id", "")
                        dest_module_id = edge.get("consumer_module_id", "")
                        dest_component_id = edge.get("consumer_component_id", "")
                    else:
                        # Edge goes same direction as path
                        # source (requester) = consumer = nodes[i]
                        # destination = provider = nodes[i+1]
                        source_module_id = edge.get("consumer_module_id", "")
                        source_component_id = edge.get("consumer_component_id", "")
                        dest_module_id = edge.get("provider_module_id", "")
                        dest_component_id = edge.get("provider_component_id", "")

                    if i == 0:
                        if request.start.module_rsm_id:
                            source_module_id = request.start.module_rsm_id
                        if request.start.component_rsm_id:
                            source_component_id = request.start.component_rsm_id

                    if i == num_nodes - 2:
                        if request.finish.module_rsm_id:
                            dest_module_id = request.finish.module_rsm_id
                        if request.finish.component_rsm_id:
                            dest_component_id = request.finish.component_rsm_id

                source_names = node_names.get((source_node, source_module_id, source_component_id))
                source_system_names = node_names.get((source_node, "", ""))
                dest_names = node_names.get((dest_node, dest_module_id, dest_component_id))
                dest_system_names = node_names.get((dest_node, "", ""))

                segments.append(PathSegment(
                    description="",
                    source=PathSegmentSource(
                        system_rsm_id=source_node,
                        system_rsm_name=source_system_names.get("system_rsm_name") if source_system_names else None,
                        module_rsm_id=source_module_id,
                        module_rsm_name=source_names.get("module_rsm_name") if source_names else None,
                        component_rsm_id=source_component_id,
                        component_rsm_name=source_names.get("component_rsm_name") if source_names else None,
                    ),
                    destination=PathSegmentDestination(
                        system_rsm_id=dest_node,
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