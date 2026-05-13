import logging
from typing import Optional

import asyncpg

from app.core.config import settings

logger = logging.getLogger(__name__)

pool: Optional[asyncpg.Pool] = None


async def init_age_session(conn: asyncpg.Connection) -> None:
    await conn.execute("LOAD 'age';")
    await conn.execute('SET search_path = ag_catalog, "$user", public;')


async def create_pool() -> asyncpg.Pool:
    global pool
    logger.info("Creating database connection pool...")
    
    pool = await asyncpg.create_pool(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        min_size=settings.POSTGRES_POOL_MIN_SIZE,
        max_size=settings.POSTGRES_POOL_MAX_SIZE,
        init=init_age_session,
    )
    
    logger.info(
        "Database connection pool created",
        extra={"min_size": settings.POSTGRES_POOL_MIN_SIZE, "max_size": settings.POSTGRES_POOL_MAX_SIZE}
    )
    return pool


async def close_pool() -> None:
    global pool
    if pool:
        logger.info("Closing database connection pool...")
        await pool.close()
        pool = None
        logger.info("Database connection pool closed")


def get_pool() -> asyncpg.Pool:
    if pool is None:
        raise RuntimeError("Database pool not initialized")
    return pool


async def check_database_ready() -> bool:
    if pool is None:
        return False
    
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database readiness check failed: {e}")
        return False
