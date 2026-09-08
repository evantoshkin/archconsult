import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config as NebulaConfig

from app.core.config import settings

logger = logging.getLogger(__name__)

_nebula_pool: Optional[ConnectionPool] = None
_pool_healthy: bool = False
_last_reinit_at: float = 0.0
_reinit_async_lock: Optional[asyncio.Lock] = None

_db_executor = ThreadPoolExecutor(
    max_workers=10,
    thread_name_prefix="nebula-db",
)


async def run_in_executor(fn, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_db_executor, fn, *args)


def _build_config() -> NebulaConfig:
    config = NebulaConfig()
    config.max_connection_pool_size = settings.DB_POOL_MAX_SIZE
    config.min_connection_pool_size = settings.DB_POOL_MIN_SIZE
    config.idle_time = settings.DB_IDLE_TIME_SEC
    config.interval_check = settings.DB_INTERVAL_CHECK_SEC
    config.timeout = settings.DB_STATEMENT_TIMEOUT_MS
    config.connect_timeout = settings.DB_CONNECT_TIMEOUT_MS
    config.check_schema = True
    config.schema_timeout = settings.DB_SCHEMA_CHECK_MS
    return config


async def create_nebula_pool() -> ConnectionPool:
    global _nebula_pool, _pool_healthy

    logger.info(f"Creating NebulaGraph connection pool to {settings.NEBULA_HOST}:{settings.NEBULA_PORT}")

    config = _build_config()
    pool = ConnectionPool()

    def _init():
        return pool.init([(settings.NEBULA_HOST, settings.NEBULA_PORT)], config)

    ok = await run_in_executor(_init)
    if not ok:
        raise RuntimeError("Failed to initialize NebulaGraph connection pool")

    _nebula_pool = pool
    _pool_healthy = True
    logger.info("NebulaGraph connection pool created")
    return pool


async def reinit_nebula_pool() -> None:
    """Best-effort teardown+recreate of the pool inside the cooldown window."""
    global _nebula_pool, _pool_healthy, _last_reinit_at

    now = time.monotonic()
    if now - _last_reinit_at < settings.DB_REINIT_COOLDOWN_SEC:
        logger.warning("Skipping pool re-init: within re-init cooldown")
        return
    _last_reinit_at = now

    pool = _nebula_pool
    _nebula_pool = None
    _pool_healthy = False

    try:
        if pool:
            await run_in_executor(pool.close)
    except Exception as e:
        logger.warning(f"Error closing stale NebulaGraph pool: {e}")

    try:
        await create_nebula_pool()
    except Exception as e:
        logger.error(f"Failed to recreate NebulaGraph pool: {e}")
        _pool_healthy = False


def get_nebula_pool() -> ConnectionPool:
    if _nebula_pool is None:
        raise RuntimeError("NebulaGraph connection pool not initialized")
    return _nebula_pool


def get_pool_healthy() -> bool:
    return _pool_healthy


async def mark_pool_unhealthy() -> None:
    global _pool_healthy
    _pool_healthy = False
    logger.warning("Marked NebulaGraph pool unhealthy; next request will re-init")


async def ensure_healthy_pool() -> bool:
    """Return True if the pool is healthy, re-initialising it when needed."""
    global _nebula_pool, _pool_healthy, _reinit_async_lock

    if _nebula_pool is not None and _pool_healthy:
        return True

    if _reinit_async_lock is None:
        _reinit_async_lock = asyncio.Lock()

    async with _reinit_async_lock:
        if _nebula_pool is not None and _pool_healthy:
            return True
        try:
            await reinit_nebula_pool()
        except Exception as e:
            logger.error(f"ensure_healthy_pool re-init failed: {e}")
            _pool_healthy = False
        return _pool_healthy


async def heartbeat():
    """Background task keeping the pool alive and detecting dead connections."""
    while True:
        await asyncio.sleep(settings.DB_HEARTBEAT_SEC)
        if _nebula_pool is None:
            continue
        try:
            ok = await asyncio.wait_for(ping_nebula(), timeout=settings.DB_CONNECT_TIMEOUT_MS / 1000)
        except Exception as e:
            ok = False
            logger.warning(f"NebulaGraph heartbeat timed out: {e}")
        if not ok:
            logger.warning("NebulaGraph heartbeat failed; triggering re-init")
            await ensure_healthy_pool()


async def ping_nebula() -> bool:
    def _run() -> bool:
        try:
            pool = get_nebula_pool()
            session = pool.get_session(settings.NEBULA_USER, settings.NEBULA_PASSWORD)
            try:
                result = session.execute(f'USE {settings.NEBULA_SPACE};')
                return result.is_succeeded()
            finally:
                session.release()
        except Exception as e:
            logger.warning(f"Pool ping failed: {e}")
            return False

    return await run_in_executor(_run)


async def close_nebula_pool():
    global _nebula_pool, _pool_healthy

    if _nebula_pool:
        logger.info("Closing NebulaGraph connection pool")
        pool = _nebula_pool
        _nebula_pool = None
        _pool_healthy = False
        await run_in_executor(pool.close)