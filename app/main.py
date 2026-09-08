import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.v1 import paths as paths_v1
from app.api.v3 import paths as paths_v3
from app.core.logging import (
    RequestBodyLoggingMiddleware,
    RequestIdMiddleware,
    setup_logging,
)
from app.db.nebula_pool import (
    close_nebula_pool,
    create_nebula_pool,
    heartbeat,
    ping_nebula,
)

logger = logging.getLogger(__name__)

_heartbeat_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _heartbeat_task
    setup_logging()
    logger.info("Starting MCP Path Service...")

    try:
        await create_nebula_pool()
    except Exception as e:
        logger.error(f"Failed to create NebulaGraph pool: {e}")
        raise

    _heartbeat_task = asyncio.create_task(heartbeat())
    logger.info("NebulaGraph pool heartbeat started")

    yield

    logger.info("Shutting down MCP Path Service...")
    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass
    await close_nebula_pool()


app = FastAPI(
    title="MCP Path Service",
    description="REST API for path search between systems in NebulaGraph",
    version="0.1.0",
    lifespan=lifespan,
    openapi_version="3.0.3",
)

app.add_middleware(RequestIdMiddleware)
app.add_middleware(RequestBodyLoggingMiddleware)

app.include_router(paths_v1.router)
app.include_router(paths_v3.router)


@app.get("/health", tags=["health"])
async def health():
    try:
        db_ok = await asyncio.wait_for(ping_nebula(), timeout=3.0)
    except Exception:
        db_ok = False

    content = {"status": "ok" if db_ok else "degraded"}
    status_code = 200 if db_ok else 503
    return JSONResponse(status_code=status_code, content=content)


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": {"code": "INTERNAL_SERVER_ERROR", "message": "Internal server error"}},
    )