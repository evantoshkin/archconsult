from fastapi import APIRouter, HTTPException

from app.db.pool import check_database_ready
from app.schemas.paths import HealthResponse, ReadyResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get(
    "/ready",
    response_model=ReadyResponse,
    responses={503: {"description": "Service not ready"}},
)
async def readiness_check() -> ReadyResponse:
    db_ready = await check_database_ready()
    
    if not db_ready:
        raise HTTPException(
            status_code=503,
            detail={"status": "not_ready", "database": "error"},
        )
    
    return ReadyResponse(status="ready", database="ok")
