import asyncpg
import logging
from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

from app.db.queries import execute_dijkstra_search, fetch_node_names, build_child_tree_by_rsm_id
from app.schemas.paths import (
    DijkstraRequest,
    DijkstraResponse,
    DijkstraPathGroup,
    DijkstraPathNode,
    DijkstraSortBy,
    ChildTreeResponse,
)

router = APIRouter(prefix="/api/v2", tags=["paths"])


@router.post(
    "/paths",
    response_model=DijkstraResponse,
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
async def dijkstra_search(request: DijkstraRequest) -> DijkstraResponse:
    try:
        results = await execute_dijkstra_search(
            start_filter=request.start,
            finish_filter=request.finish,
        )
    except asyncpg.PostgresConnectionError:
        raise HTTPException(
            status_code=503,
            detail={"code": "DATABASE_UNAVAILABLE", "message": "Database connection error"},
        )
    except asyncpg.QueryCanceledError:
        raise HTTPException(
            status_code=504,
            detail={"code": "DATABASE_TIMEOUT", "message": "Database query timeout"},
        )
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_SERVER_ERROR", "message": "Internal server error"},
        )
    
    all_nodes: list[tuple[str, str, str]] = []
    for eotar_id, data in results.items():
        for finish_node, path_data in data["paths"].items():
            for node_id in path_data["path"]:
                parts = node_id.split("|")
                all_nodes.append((
                    parts[0] if len(parts) > 0 else "",
                    parts[1] if len(parts) > 1 else "",
                    parts[2] if len(parts) > 2 else "",
                ))
    
    node_names = await fetch_node_names(all_nodes)
    
    path_groups: dict[tuple, dict] = {}
    
    for eotar_id, result_data in results.items():
        for finish_node, data in result_data["paths"].items():
            path_key = tuple(data["path"])
            
            if path_key not in path_groups:
                named_path: list[DijkstraPathNode] = []
                for i, node_id in enumerate(data["path"]):
                    parts = node_id.split("|")
                    sys_id = parts[0] if len(parts) > 0 else ""
                    mod_id = parts[1] if len(parts) > 1 else ""
                    comp_id = parts[2] if len(parts) > 2 else ""
                    
                    names = node_names.get((sys_id, mod_id, comp_id))
                    
                    named_path.append(DijkstraPathNode(
                        order=i,
                        system_rsm_id=sys_id,
                        system_rsm_name=names.system_rsm_name if names else None,
                        module_rsm_id=mod_id,
                        module_rsm_name=names.module_rsm_name if names else None,
                        component_rsm_id=comp_id,
                        component_rsm_name=names.component_rsm_name if names else None,
                    ))
                
                path_groups[path_key] = {
                    "path": named_path,
                    "integration_example_count": 0,
                    "eotar_rsm_id": eotar_id,
                    "eotar_rsm_date_time": result_data.get("eotar_rsm_date_time"),
                }
            
            path_groups[path_key]["integration_example_count"] += 1
    
    if request.sort_by == DijkstraSortBy.MOST_FREQUENT:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: -x["integration_example_count"]
        )
    elif request.sort_by == DijkstraSortBy.LONGEST:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (-len(x["path"]), -x["integration_example_count"])
        )
    elif request.sort_by == DijkstraSortBy.SHORTEST:
        sorted_paths = sorted(
            path_groups.values(),
            key=lambda x: (len(x["path"]), -x["integration_example_count"])
        )
    elif request.sort_by == DijkstraSortBy.MOST_RECENT:
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
    
    return DijkstraResponse(
        results=[DijkstraPathGroup(**p) for p in sorted_paths]
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
        "x-mcp-tool-description": "Получение дерева дочерних объектов по RSM ID. Возвращает иерархическую структуру системы.",
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
    try:
        result = await build_child_tree_by_rsm_id(rsm_id)
    except asyncpg.PostgresConnectionError as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(
            status_code=503,
            detail={"code": "DATABASE_UNAVAILABLE", "message": "Database connection error"},
        )
    except asyncpg.QueryCanceledError as e:
        logger.error(f"Database query timeout: {e}")
        raise HTTPException(
            status_code=504,
            detail={"code": "DATABASE_TIMEOUT", "message": "Database query timeout"},
        )
    except Exception as e:
        logger.error(f"Internal error: {e}")
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_SERVER_ERROR", "message": str(e)},
        )
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "OBJECT_NOT_FOUND", "message": f"Object with rsm_id={rsm_id} not found"},
        )
    
    return ChildTreeResponse(
        node=result.node,
        children=result.children,
    )
