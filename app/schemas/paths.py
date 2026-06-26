from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class PathSearchResponse(BaseModel):
    from_system_id: str
    from_system_name: Optional[str] = None
    to_system_id: str
    to_system_name: Optional[str] = None
    path_length: int
    path: str
    frequency: int
    example_document_rsm_id: Optional[str] = None


class PathNotFoundError(BaseModel):
    code: str = "PATH_NOT_FOUND"
    message: str = "Path between systems was not found"
    from_system_id: str
    to_system_id: str


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    detail: ErrorDetail


class HealthResponse(BaseModel):
    status: str = "ok"


class ReadyResponse(BaseModel):
    status: str
    database: str


SYSTEM_ID_REGEX = r"^[a-zA-Z0-9_-]{1,128}$"


class PathQueryParams(BaseModel):
    from_system_id: str = Field(..., pattern=SYSTEM_ID_REGEX, description="RSM ID исходной системы")
    to_system_id: str = Field(..., pattern=SYSTEM_ID_REGEX, description="RSM ID целевой системы")


class SystemSearchResponse(BaseModel):
    system_id: str
    system_name: str


class SystemSearchListResponse(BaseModel):
    total: int
    systems: list[SystemSearchResponse]


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


class TraverseRequest(BaseModel):
    start: TraverseFilter = Field(
        ...,
        description="Filter for start nodes",
        json_schema_extra={
            "x-mcp-tool-arg-name": "start",
            "x-mcp-tool-arg-description": "Фильтр для начальных узлов поиска пути",
        }
    )
    finish: TraverseFilter = Field(
        ...,
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


class TraversePathNode(BaseModel):
    order: int
    system_rsm_id: str
    system_rsm_name: Optional[str] = None
    module_rsm_id: str = ""
    module_rsm_name: Optional[str] = None
    component_rsm_id: str = ""
    component_rsm_name: Optional[str] = None


class TraversePathGroup(BaseModel):
    path: list[TraversePathNode]
    integration_example_count: int
    document_rsm_id: str
    document_rsm_date_time: Optional[str] = None


class TraverseResponse(BaseModel):
    paths: list[TraversePathGroup]


class ExperimentNodePath(BaseModel):
    system_rsm_id: str
    system_rsm_name: Optional[str] = None
    frequency: int = 1


class ExperimentNodePaths(BaseModel):
    incoming: list[ExperimentNodePath] = []
    outgoing: list[ExperimentNodePath] = []


class ExperimentPathSegmentSource(BaseModel):
    system_rsm_id: str
    system_rsm_name: Optional[str] = None
    module_rsm_id: str = ""
    module_rsm_name: Optional[str] = None
    component_rsm_id: str = ""
    component_rsm_name: Optional[str] = None
    paths: Optional[ExperimentNodePaths] = None


class ExperimentPathSegmentDestination(BaseModel):
    system_rsm_id: str
    system_rsm_name: Optional[str] = None
    module_rsm_id: str = ""
    module_rsm_name: Optional[str] = None
    component_rsm_id: str = ""
    component_rsm_name: Optional[str] = None
    paths: Optional[ExperimentNodePaths] = None


class ExperimentPathSegment(BaseModel):
    source: ExperimentPathSegmentSource
    destination: ExperimentPathSegmentDestination


class ExperimentPathGroup(BaseModel):
    segments: list[ExperimentPathSegment]
    frequency: int
    document_rsm_id: str
    document_rsm_date_time: Optional[str] = None


class ExperimentResponse(BaseModel):
    paths: list[ExperimentPathGroup]


class ChildNode(BaseModel):
    properties: dict = {}


class ChildTreeItem(BaseModel):
    node: ChildNode
    children: list["ChildTreeItem"] = []


class ChildTreeResponse(BaseModel):
    node: ChildNode
    children: list[ChildTreeItem] = []


ChildTreeItem.model_rebuild()
