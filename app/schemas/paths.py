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
    example_eotar_rsm_id: Optional[str] = None


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


class DijkstraFilter(BaseModel):
    system_rsm_id: Optional[str] = None
    module_rsm_id: Optional[str] = None
    component_rsm_id: Optional[str] = None


class DijkstraSortBy(str, Enum):
    MOST_FREQUENT = "most_frequent"
    LONGEST = "longest"
    SHORTEST = "shortest"
    MOST_RECENT = "most_recent"


class DijkstraRequest(BaseModel):
    start: DijkstraFilter = Field(..., description="Filter for start nodes")
    finish: DijkstraFilter = Field(..., description="Filter for finish nodes")
    sort_by: DijkstraSortBy = Field(
        default=DijkstraSortBy.MOST_FREQUENT,
        description="Sort order: most_frequent, longest, or shortest"
    )


class DijkstraPathNode(BaseModel):
    order: int
    system_rsm_id: str
    system_rsm_name: Optional[str] = None
    module_rsm_id: str = ""
    module_rsm_name: Optional[str] = None
    component_rsm_id: str = ""
    component_rsm_name: Optional[str] = None


class DijkstraPathGroup(BaseModel):
    path: list[DijkstraPathNode]
    integration_example_count: int
    eotar_rsm_id: str
    eotar_rsm_date_time: Optional[str] = None


class DijkstraResponse(BaseModel):
    results: list[DijkstraPathGroup]


class ChildNode(BaseModel):
    properties: dict = {}


class ChildTreeResponse(BaseModel):
    node: ChildNode
    children: list["ChildTreeResponse"] = []


ChildTreeResponse.model_rebuild()
