import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config as NebulaConfig

from app.core.config import settings

logger = logging.getLogger(__name__)

_nebula_pool: Optional[ConnectionPool] = None

_db_executor = ThreadPoolExecutor(
    max_workers=10,
    thread_name_prefix="nebula-db",
)


async def run_in_executor(fn, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_db_executor, fn, *args)


async def create_nebula_pool() -> ConnectionPool:
    global _nebula_pool

    logger.info(f"Creating NebulaGraph connection pool to {settings.NEBULA_HOST}:{settings.NEBULA_PORT}")

    config = NebulaConfig()
    config.max_connection_pool_size = settings.DB_POOL_MAX_SIZE
    config.min_connection_pool_size = settings.DB_POOL_MIN_SIZE
    config.idle_time = 0
    config.interval_check = 30
    config.timeout = 30000

    pool = ConnectionPool()

    def _init():
        return pool.init([(settings.NEBULA_HOST, settings.NEBULA_PORT)], config)

    ok = await run_in_executor(_init)
    if not ok:
        raise RuntimeError("Failed to initialize NebulaGraph connection pool")

    _nebula_pool = pool
    logger.info("NebulaGraph connection pool created")
    return pool


def get_nebula_pool() -> ConnectionPool:
    if _nebula_pool is None:
        raise RuntimeError("NebulaGraph connection pool not initialized")
    return _nebula_pool


async def close_nebula_pool():
    global _nebula_pool

    if _nebula_pool:
        logger.info("Closing NebulaGraph connection pool")
        pool = _nebula_pool
        _nebula_pool = None
        await run_in_executor(pool.close)