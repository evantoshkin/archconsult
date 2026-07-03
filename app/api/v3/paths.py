import logging
from fastapi import APIRouter, HTTPException

from app.core.config import settings
from app.db.nebula_pool import get_nebula_pool
from app.db.nebula_queries import execute_nebula_experiment_search, fetch_nebula_node_names
from app.schemas.paths import (
    ExperimentRequest,
    ExperimentResponse,
    ExperimentPathGroup,
    ExperimentPathSegment,
    ExperimentPathSegmentSource,
    ExperimentPathSegmentDestination,
    ExperimentNodePaths,
    ExperimentNodePath,
    TraverseSortBy,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v3", tags=["paths"])


def fetch_node_paths_with_segment_filter(session, source_node: str, dest_node: str, document_id: str, search_incoming: bool = True, search_outgoing: bool = True) -> tuple[dict, dict]:
    source_result = {
        "incoming": [],
        "outgoing": [],
    }
    dest_result = {
        "incoming": [],
        "outgoing": [],
    }

    segment_query = f'GO FROM "{source_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{document_id}" AND VISION_INTERFACE_SYSTEM_LEVEL._dst == "{dest_node}" YIELD VISION_INTERFACE_SYSTEM_LEVEL._dst AS dst'
    logger.info(f"Checking segment {source_node} -> {dest_node} with document_id={document_id}")
    segment_result = session.execute(segment_query)

    if not (segment_result.is_succeeded() and segment_result.row_size() > 0):
        logger.info(f"Segment {source_node} -> {dest_node} not found with document_id={document_id}")
        return source_result, dest_result

    if search_incoming:
        source_incoming_map: dict[str, dict] = {}
        source_incoming_query = f'GO FROM "{source_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL REVERSELY WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{document_id}" YIELD id($$) AS src_id, $$.SYSTEM.name AS src_name'
        logger.info(f"Fetching incoming paths for source node {source_node} with document_id={document_id}")
        source_incoming_result = session.execute(source_incoming_query)

        if not source_incoming_result.is_succeeded():
            logger.error(f"Source incoming query failed: {source_incoming_result.error_msg()}")

        if source_incoming_result.is_succeeded():
            for row_idx in range(source_incoming_result.row_size()):
                row = source_incoming_result.row_values(row_idx)
                src_id = str(row[0]).strip('"') if row[0] else None
                system_name = str(row[1]).strip('"') if row[1] and str(row[1]) not in ["None", "__EMPTY__", '"NULL"'] else None

                if src_id and src_id != source_node:
                    if src_id not in source_incoming_map:
                        source_incoming_map[src_id] = {
                            "system_rsm_id": src_id,
                            "system_rsm_name": system_name,
                            "frequency": 0,
                        }
                    source_incoming_map[src_id]["frequency"] += 1
        source_result["incoming"] = sorted(source_incoming_map.values(), key=lambda x: -x["frequency"])[:3]

    if search_outgoing:
        source_outgoing_map: dict[str, dict] = {}
        source_outgoing_query = f'GO 1 STEPS FROM "{source_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL YIELD id($$) AS src_id, $$.SYSTEM.name AS src_name'
        logger.info(f"Fetching outgoing paths for source node {source_node}")
        source_outgoing_result = session.execute(source_outgoing_query)

        if source_outgoing_result.is_succeeded():
            for row_idx in range(source_outgoing_result.row_size()):
                row = source_outgoing_result.row_values(row_idx)
                src_id = str(row[0]).strip('"') if row[0] else None
                system_name = str(row[1]).strip('"') if row[1] and str(row[1]) not in ["None", "__EMPTY__", '"NULL"'] else None

                if src_id and src_id != source_node and src_id != dest_node:
                    if src_id not in source_outgoing_map:
                        source_outgoing_map[src_id] = {
                            "system_rsm_id": src_id,
                            "system_rsm_name": system_name,
                            "frequency": 0,
                        }
                    source_outgoing_map[src_id]["frequency"] += 1
        source_result["outgoing"] = sorted(source_outgoing_map.values(), key=lambda x: -x["frequency"])[:3]

    if search_incoming:
        dest_incoming_map: dict[str, dict] = {}
        dest_incoming_query = f'GO FROM "{dest_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL REVERSELY WHERE VISION_INTERFACE_SYSTEM_LEVEL.rsm_document_id == "{document_id}" YIELD id($$) AS src_id, $$.SYSTEM.name AS src_name'
        logger.info(f"Fetching incoming paths for destination node {dest_node}")
        dest_incoming_result = session.execute(dest_incoming_query)

        if dest_incoming_result.is_succeeded():
            for row_idx in range(dest_incoming_result.row_size()):
                row = dest_incoming_result.row_values(row_idx)
                src_id = str(row[0]).strip('"') if row[0] else None
                system_name = str(row[1]).strip('"') if row[1] and str(row[1]) not in ["None", "__EMPTY__", '"NULL"'] else None

                if src_id and src_id != dest_node and src_id != source_node:
                    if src_id not in dest_incoming_map:
                        dest_incoming_map[src_id] = {
                            "system_rsm_id": src_id,
                            "system_rsm_name": system_name,
                            "frequency": 0,
                        }
        dest_result["incoming"] = sorted(dest_incoming_map.values(), key=lambda x: -x["frequency"])[:3]
        dest_result["incoming"] = sorted(dest_incoming_map.values(), key=lambda x: -x["frequency"])

    if search_outgoing:
        dest_outgoing_map: dict[str, dict] = {}
        dest_outgoing_query = f'GO 1 STEPS FROM "{dest_node}" OVER VISION_INTERFACE_SYSTEM_LEVEL YIELD id($$) AS src_id, $$.SYSTEM.name AS src_name'
        logger.info(f"Fetching outgoing paths for destination node {dest_node}")
        dest_outgoing_result = session.execute(dest_outgoing_query)

        if dest_outgoing_result.is_succeeded():
            for row_idx in range(dest_outgoing_result.row_size()):
                row = dest_outgoing_result.row_values(row_idx)
                src_id = str(row[0]).strip('"') if row[0] else None
                system_name = str(row[1]).strip('"') if row[1] and str(row[1]) not in ["None", "__EMPTY__", '"NULL"'] else None

                if src_id and src_id != dest_node:
                    if src_id not in dest_outgoing_map:
                        dest_outgoing_map[src_id] = {
                            "system_rsm_id": src_id,
                            "system_rsm_name": system_name,
                            "frequency": 0,
                        }
                    dest_outgoing_map[src_id]["frequency"] += 1
        dest_result["outgoing"] = sorted(dest_outgoing_map.values(), key=lambda x: -x["frequency"])[:3]

    return source_result, dest_result


@router.post(
    "/paths",
    response_model=ExperimentResponse,
    responses={
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
    openapi_extra={
        "x-mcp-tool-name": "build_experiment_path",
        "x-mcp-tool-description": "Экспериментальный поиск путей интеграции. Возвращает отрезки source -> destination.",
    },
)
async def experiment_search(request: ExperimentRequest) -> ExperimentResponse:
    try:
        results = await execute_nebula_experiment_search(
            start_filter=request.start,
            finish_filter=request.finish,
            depth_days=request.depth_days,
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
            num_nodes = len(path_nodes)
            document_id = sorted(eotar_ids)[0] if eotar_ids else ""

            segments: list[ExperimentPathSegment] = []
            for i in range(num_nodes - 1):
                source_node = path_nodes[i]
                dest_node = path_nodes[i + 1]

                source_module_id = ""
                source_component_id = ""
                dest_module_id = ""
                dest_component_id = ""

                if i < len(edge_data_list):
                    edge = edge_data_list[i]
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

                source_paths_data, dest_paths_data = fetch_node_paths_with_segment_filter(
                    session, source_node, dest_node, document_id,
                    search_incoming=request.search_incoming,
                    search_outgoing=request.search_outgoing
                )

                source_paths = ExperimentNodePaths(
                    incoming=[ExperimentNodePath(**p) for p in source_paths_data.get("incoming", [])],
                    outgoing=[ExperimentNodePath(**p) for p in source_paths_data.get("outgoing", [])],
                )

                dest_paths = ExperimentNodePaths(
                    incoming=[ExperimentNodePath(**p) for p in dest_paths_data.get("incoming", [])],
                    outgoing=[ExperimentNodePath(**p) for p in dest_paths_data.get("outgoing", [])],
                )

                segments.append(ExperimentPathSegment(
                    source=ExperimentPathSegmentSource(
                        system_rsm_id=source_node,
                        system_rsm_name=source_system_names.get("system_rsm_name") if source_system_names else None,
                        module_rsm_id=source_module_id,
                        module_rsm_name=source_names.get("module_rsm_name") if source_names else None,
                        component_rsm_id=source_component_id,
                        component_rsm_name=source_names.get("component_rsm_name") if source_names else None,
                        paths=source_paths,
                    ),
                    destination=ExperimentPathSegmentDestination(
                        system_rsm_id=dest_node,
                        system_rsm_name=dest_system_names.get("system_rsm_name") if dest_system_names else None,
                        module_rsm_id=dest_module_id,
                        module_rsm_name=dest_names.get("module_rsm_name") if dest_names else None,
                        component_rsm_id=dest_component_id,
                        component_rsm_name=dest_names.get("component_rsm_name") if dest_names else None,
                        paths=dest_paths,
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

    return ExperimentResponse(
        paths=[ExperimentPathGroup(**p) for p in sorted_paths[:request.path_count]]
    )