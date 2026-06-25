import logging
from typing import Optional

from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config as NebulaConfig

from app.core.config import settings

logger = logging.getLogger(__name__)

_nebula_pool: Optional[ConnectionPool] = None


async def create_nebula_pool() -> ConnectionPool:
    global _nebula_pool
    
    logger.info(f"Creating NebulaGraph connection pool to {settings.NEBULA_HOST}:{settings.NEBULA_PORT}")
    
    config = NebulaConfig()
    config.max_connection_pool_size = 10
    
    pool = ConnectionPool()
    pool.init([(settings.NEBULA_HOST, settings.NEBULA_PORT)], config)
    
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
        _nebula_pool.close()
        _nebula_pool = None
