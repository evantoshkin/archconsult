from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    NEBULA_HOST: str = "localhost"
    NEBULA_PORT: int = 9670
    NEBULA_USER: str = "root"
    NEBULA_PASSWORD: str = "nebula"
    NEBULA_SPACE: str = "RSM"

    SEARCH_DEPTH_DAYS: int = 30
    MAX_PATH_DEPTH: int = 7
    DB_POOL_MAX_SIZE: int = 10
    DB_POOL_MIN_SIZE: int = 2

    # NebulaGraph client pool tuning (ms / seconds).
    DB_CONNECT_TIMEOUT_MS: int = 5000
    DB_STATEMENT_TIMEOUT_MS: int = 30000
    DB_IDLE_TIME_SEC: int = 3600
    DB_INTERVAL_CHECK_SEC: int = 5
    DB_SCHEMA_CHECK_MS: int = 1000
    DB_REINIT_COOLDOWN_SEC: float = 5.0

    # Keep-alive / retry behaviour.
    DB_HEARTBEAT_SEC: int = 30
    DB_RETRY_COUNT: int = 2

    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
