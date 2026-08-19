from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from app.core.config import settings


class TraverseFilter(BaseModel):
    system_rsm_id: Optional[str] = Field(
        None,
        json_schema_extra={
            "x-mcp-tool-arg-name": "system_rsm_id",
            "x-mcp-tool-arg-description": "RSM ID системы для фильтрации",
        }
    )
    module_rsm_id: Optional[str] = Field(
        None,
        json_schema_extra={
            "x-mcp-tool-arg-name": "module_rsm_id",
            "x-mcp-tool-arg-description": "RSM ID модуля для фильтрации",
        }
    )
    component_rsm_id: Optional[str] = Field(
        None,
        json_schema_extra={
            "x-mcp-tool-arg-name": "component_rsm_id",
            "x-mcp-tool-arg-description": "RSM ID компонента для фильтрации",
        }
    )


class TraverseSortBy(str, Enum):
    MOST_FREQUENT = "most_frequent"
    LONGEST = "longest"
    SHORTEST = "shortest"
    MOST_RECENT = "most_recent"


class SourceType(str, Enum):
    VISION = "vision"
    INTERFACE_REGISTRY = "interface_registry"


class PathRequest(BaseModel):
    source: SourceType = Field(
        default=SourceType.VISION,
        description="Source type for path search: vision (VISION_INTERFACE_SYSTEM_LEVEL) or interface_registry (INTERFACE_REGISTRY_INTERFACE_SYSTEM_LEVEL)",
        json_schema_extra={
            "x-mcp-tool-arg-name": "source",
            "x-mcp-tool-arg-description": "Источник данных: vision (VISION_INTERFACE_SYSTEM_LEVEL) или interface_registry (INTERFACE_REGISTRY_INTERFACE_SYSTEM_LEVEL)",
        }
    )
    start: Optional[TraverseFilter] = Field(
        None,
        description="Filter for start nodes",
        json_schema_extra={
            "x-mcp-tool-arg-name": "start",
            "x-mcp-tool-arg-description": "Фильтр для начальных узлов поиска пути",
        }
    )
    finish: Optional[TraverseFilter] = Field(
        None,
        description="Filter for finish nodes",
        json_schema_extra={
            "x-mcp-tool-arg-name": "finish",
            "x-mcp-tool-arg-description": "Фильтр для конечных узлов поиска пути",
        }
    )
    sort_by: TraverseSortBy = Field(
        default=TraverseSortBy.MOST_FREQUENT,
        description="Sort order: most_frequent, longest, or shortest",
        json_schema_extra={
            "x-mcp-tool-arg-name": "sort_by",
            "x-mcp-tool-arg-description": "Порядок сортировки: most_frequent (по частоте), longest (самые длинные), shortest (самые короткие), most_recent (недавние)",
        }
    )
    path_count: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of paths to return",
        json_schema_extra={
            "x-mcp-tool-arg-name": "path_count",
            "x-mcp-tool-arg-description": "Количество путей в ответе (от 1 до 100)",
        }
    )
    depth_days: int = Field(
        default=settings.SEARCH_DEPTH_DAYS,
        ge=1,
        le=365,
        description="Depth of search in days",
        json_schema_extra={
            "x-mcp-tool-arg-name": "depth_days",
            "x-mcp-tool-arg-description": "Количество дней глубины поиска (от 1 до 365)",
        }
    )


class PathSegmentSource(BaseModel):
    system_rsm_id: str = ""
    system_rsm_name: Optional[str] = None
    module_rsm_id: str = ""
    module_rsm_name: Optional[str] = None
    component_rsm_id: str = ""
    component_rsm_name: Optional[str] = None


class PathSegmentDestination(BaseModel):
    system_rsm_id: str = ""
    system_rsm_name: Optional[str] = None
    module_rsm_id: str = ""
    module_rsm_name: Optional[str] = None
    component_rsm_id: str = ""
    component_rsm_name: Optional[str] = None


class PathSegment(BaseModel):
    source: PathSegmentSource
    destination: PathSegmentDestination
    description: str = ""


class PathGroup(BaseModel):
    segments: list[PathSegment] = []
    description: str = ""


class PathResponse(BaseModel):
    paths: list[PathGroup]

class ChildNode(BaseModel):
    label: str = ""
    rsm_id: str = ""
    rsm_name: Optional[str] = None
    description: Optional[str] = None


class ChildTreeItem(BaseModel):
    node: ChildNode
    children: list["ChildTreeItem"] = []


class ChildTreeResponse(BaseModel):
    node: ChildNode
    children: list[ChildTreeItem] = []


ChildTreeItem.model_rebuild()
