import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query

from app.db.queries import execute_path_search, execute_path_search_bounded, search_systems_by_name, DEFAULT_MAX_DEPTH
from app.schemas.paths import PathNotFoundError, PathSearchResponse, SYSTEM_ID_REGEX, SystemSearchListResponse, SystemSearchResponse

router = APIRouter(prefix="/api/v1", tags=["paths"])


@router.get(
    "/paths",
    response_model=PathSearchResponse,
    responses={
        404: {"model": PathNotFoundError, "description": "Path not found"},
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
)
async def get_path(
    from_system_id: str = Query(..., pattern=SYSTEM_ID_REGEX, description="RSM ID исходной системы"),
    to_system_id: str = Query(..., pattern=SYSTEM_ID_REGEX, description="RSM ID целевой системы"),
) -> PathSearchResponse:
    try:
        result = await execute_path_search(from_system_id, to_system_id)
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
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=PathNotFoundError(
                from_system_id=from_system_id,
                to_system_id=to_system_id,
            ).model_dump(),
        )
    
    return PathSearchResponse(
        from_system_id=result.from_system_id,
        from_system_name=result.from_system_name,
        to_system_id=result.to_system_id,
        to_system_name=result.to_system_name,
        path_length=result.path_length,
        path=result.path,
        frequency=result.frequency,
        example_eotar_rsm_id=result.example_eotar_rsm_id,
    )


@router.get(
    "/paths/bounded",
    response_model=PathSearchResponse,
    responses={
        404: {"model": PathNotFoundError, "description": "Path not found"},
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
)
async def get_path_bounded(
    from_system_id: str = Query(..., pattern=SYSTEM_ID_REGEX, description="RSM ID исходной системы"),
    to_system_id: str = Query(..., pattern=SYSTEM_ID_REGEX, description="RSM ID целевой системы"),
    max_depth: int = Query(..., ge=1, le=100, description="Максимальная глубина поиска пути"),
) -> PathSearchResponse:
    try:
        result = await execute_path_search_bounded(from_system_id, to_system_id, max_depth)
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
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_PARAMETER", "message": str(e)},
        )
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_SERVER_ERROR", "message": "Internal server error"},
        )
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=PathNotFoundError(
                from_system_id=from_system_id,
                to_system_id=to_system_id,
            ).model_dump(),
        )
    
    return PathSearchResponse(
        from_system_id=result.from_system_id,
        from_system_name=result.from_system_name,
        to_system_id=result.to_system_id,
        to_system_name=result.to_system_name,
        path_length=result.path_length,
        path=result.path,
        frequency=result.frequency,
        example_eotar_rsm_id=result.example_eotar_rsm_id,
    )


@router.get(
    "/paths/default",
    response_model=PathSearchResponse,
    responses={
        404: {"model": PathNotFoundError, "description": "Path not found"},
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
)
async def get_path_default(
    from_system_id: str = Query(..., pattern=SYSTEM_ID_REGEX, description="RSM ID исходной системы"),
    to_system_id: str = Query(..., pattern=SYSTEM_ID_REGEX, description="RSM ID целевой системы"),
) -> PathSearchResponse:
    try:
        result = await execute_path_search_bounded(from_system_id, to_system_id, DEFAULT_MAX_DEPTH)
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
    
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=PathNotFoundError(
                from_system_id=from_system_id,
                to_system_id=to_system_id,
            ).model_dump(),
        )
    
    return PathSearchResponse(
        from_system_id=result.from_system_id,
        from_system_name=result.from_system_name,
        to_system_id=result.to_system_id,
        to_system_name=result.to_system_name,
        path_length=result.path_length,
        path=result.path,
        frequency=result.frequency,
        example_eotar_rsm_id=result.example_eotar_rsm_id,
    )


@router.get(
    "/systems/search",
    response_model=SystemSearchListResponse,
    responses={
        503: {"description": "Database unavailable"},
        504: {"description": "Database timeout"},
        500: {"description": "Internal server error"},
    },
)
async def search_systems(
    name: str = Query(..., min_length=1, max_length=256, description="Паттерн для поиска по названию системы"),
) -> SystemSearchListResponse:
    try:
        results = await search_systems_by_name(name)
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
    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail={"code": "INVALID_PARAMETER", "message": str(e)},
        )
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={"code": "INTERNAL_SERVER_ERROR", "message": "Internal server error"},
        )
    
    return SystemSearchListResponse(
        total=len(results),
        systems=[
            SystemSearchResponse(system_id=s.system_id, system_name=s.system_name)
            for s in results
        ],
    )
