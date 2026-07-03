import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.v1 import health, paths as paths_v1
from app.api.v2 import paths as paths_v2
from app.api.v3 import paths as paths_v3
from app.core.config import settings
from app.core.logging import RequestIdMiddleware, setup_logging
from app.db.pool import close_pool, create_pool
from app.db.nebula_pool import create_nebula_pool, close_nebula_pool

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logger.info("Starting MCP Path Service...")
    
    try:
        await create_pool()
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        raise
    
    try:
        await create_nebula_pool()
    except Exception as e:
        logger.error(f"Failed to create NebulaGraph pool: {e}")
        raise
    
    yield
    
    logger.info("Shutting down MCP Path Service...")
    await close_pool()
    await close_nebula_pool()


app = FastAPI(
    title="MCP Path Service",
    description="REST API for path search between systems in PostgreSQL + Apache AGE",
    version="0.1.0",
    lifespan=lifespan,
    openapi_version="3.0.3",
)

app.add_middleware(RequestIdMiddleware)

app.include_router(paths_v1.router)
app.include_router(paths_v2.router)
app.include_router(paths_v3.router)
app.include_router(health.router)


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": {"code": "INTERNAL_SERVER_ERROR", "message": "Internal server error"}},
    )
