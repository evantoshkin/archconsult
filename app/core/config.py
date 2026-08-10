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
    MAX_PATH_DEPTH: int = 10

    LOG_LEVEL: str = "INFO"
    DB_STATEMENT_TIMEOUT_MS: int = 30000

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
