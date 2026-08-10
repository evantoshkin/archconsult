import logging

from fastapi import APIRouter, HTTPException, Query

from app.db.nebula_queries import fetch_child_tree_from_nebula
from app.schemas.paths import ChildNode, ChildTreeItem, ChildTreeResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["paths"])


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
