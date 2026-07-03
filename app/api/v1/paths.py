import asyncpg
import logging
from fastapi import APIRouter, HTTPException, Query

from app.core.config import settings
from app.db.nebula_pool import get_nebula_pool
from app.db.nebula_queries import execute_nebula_traverse_search, fetch_nebula_node_names
from app.db.queries import build_child_tree_by_rsm_id

logger = logging.getLogger(__name__)
from app.schemas.paths import (
    TraverseRequest,
    TraverseResponse,
    TraversePathGroup,
    TraversePathNode,
    TraverseSortBy,
    ChildTreeResponse,
    ChildNode,
    ChildTreeItem,
)


router = APIRouter(prefix="/api/v1", tags=["paths"])


@router.post(
    "/paths",
    response_model=TraverseResponse,
    responses={
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
    openapi_extra={
        "x-mcp-tool-name": "build_path",
        "x-mcp-tool-description": "Поиск путей интеграции между системами по алгоритму Дейкстры. Возвращает отсортированные маршруты с частотой использования.",
    },
)
async def traverse_search(request: TraverseRequest) -> TraverseResponse:
    try:
        results = await execute_nebula_traverse_search(
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
    
    for path_key, eotar_ids in path_eotar_map.items():
        data = path_data_map[path_key]
        path_nodes = data["path"]
        edge_data_list = data.get("edge_data", [])
        num_nodes = len(path_nodes)
        
        named_path: list[TraversePathNode] = []
        for i, node_id in enumerate(path_nodes):
                module_rsm_id = ""
                component_rsm_id = ""
                
                if i == 0 and num_nodes > 1 and len(edge_data_list) > 0:
                    edge = edge_data_list[0]
                    module_rsm_id = edge.get("consumer_module_id", "")
                    component_rsm_id = edge.get("consumer_component_id", "")
                    # Override with filter values if specified
                    if request.start.module_rsm_id:
                        module_rsm_id = request.start.module_rsm_id
                    if request.start.component_rsm_id:
                        component_rsm_id = request.start.component_rsm_id
                elif i == num_nodes - 1 and num_nodes > 1 and len(edge_data_list) >= num_nodes - 1:
                    edge = edge_data_list[num_nodes - 2]
                    module_rsm_id = edge.get("provider_module_id", "")
                    component_rsm_id = edge.get("provider_component_id", "")
                    # Override with filter values if specified
                    if request.finish.module_rsm_id:
                        module_rsm_id = request.finish.module_rsm_id
                    if request.finish.component_rsm_id:
                        component_rsm_id = request.finish.component_rsm_id
                elif i > 0 and i < num_nodes - 1:
                    if len(edge_data_list) >= i:
                        edge = edge_data_list[i - 1]
                        module_rsm_id = edge.get("provider_module_id", "")
                        component_rsm_id = edge.get("provider_component_id", "")
                
                names = node_names.get((node_id, module_rsm_id, component_rsm_id))
                system_names = node_names.get((node_id, "", ""))
                
                named_path.append(TraversePathNode(
                    order=i,
                    system_rsm_id=node_id,
                    system_rsm_name=system_names.get("system_rsm_name") if system_names else None,
                    module_rsm_id=module_rsm_id,
                    module_rsm_name=names.get("module_rsm_name") if names else None,
                    component_rsm_id=component_rsm_id,
                    component_rsm_name=names.get("component_rsm_name") if names else None,
                ))
        
        sorted_eotar_ids = sorted(eotar_ids)
        first_eotar_id = sorted_eotar_ids[0] if sorted_eotar_ids else ""
        
        path_groups[path_key] = {
            "path": named_path,
            "integration_example_count": len(eotar_ids),
            "eotar_rsm_id": first_eotar_id,
            "eotar_rsm_date_time": data.get("document_rsm_date_time"),
        }
    
    if request.sort_by == TraverseSortBy.MOST_FREQUENT:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: -x["integration_example_count"]
        )
    elif request.sort_by == TraverseSortBy.LONGEST:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (-len(x["path"]), -x["integration_example_count"])
        )
    elif request.sort_by == TraverseSortBy.SHORTEST:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (len(x["path"]), -x["integration_example_count"])
        )
    elif request.sort_by == TraverseSortBy.MOST_RECENT:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (x.get("eotar_rsm_date_time") is None, x.get("eotar_rsm_date_time") or "", -x["integration_example_count"]),
            reverse=True
        )
    else:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: -x["integration_example_count"]
        )
    
    return TraverseResponse(
        paths=[TraversePathGroup(**p) for p in sorted_paths[:request.path_count]]
    )


@router.get(
    "/object/childAll",
    response_model=ChildTreeResponse,
    responses={
        404: {"description": "Object not found"},
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
    openapi_extra={
        "x-mcp-tool-name": "get_systemtree",
        "x-mcp-tool-description": "Получение дерева дочерних объектов по RSM ID из NebulaGraph. Возвращает иерархическую структуру системы.",
    },
)
async def get_child_tree(
    rsm_id: str = Query(
        ...,
        openapi_extra={
            "x-mcp-tool-arg-name": "rsm_id",
            "x-mcp-tool-arg-description": "RSM ID объекта для получения дерева дочерних элементов",
        }
    )
) -> ChildTreeResponse:
    from app.db.nebula_queries import fetch_child_tree_from_nebula
    
    result = await fetch_child_tree_from_nebula(rsm_id)
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "OBJECT_NOT_FOUND", "message": f"Object with rsm_id={rsm_id} not found"},
        )
    
    return ChildTreeResponse(
        node=ChildNode(**result["node"]),
        children=[ChildTreeItem(**child) for child in result["children"]],
    )



