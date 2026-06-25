from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "rsm"
    POSTGRES_USER: str = "rsm_user"
    POSTGRES_PASSWORD: str = ""
    POSTGRES_POOL_MIN_SIZE: int = 1
    POSTGRES_POOL_MAX_SIZE: int = 10

    AGE_GRAPH_NAME: str = "rsm_eotar_interface"
    PATH_SEARCH_LIMIT: int = 1000

    NEBULA_HOST: str = "localhost"
    NEBULA_PORT: int = 9670
    NEBULA_USER: str = "root"
    NEBULA_PASSWORD: str = "nebula"
    NEBULA_SPACE: str = "RSM"

    LOG_LEVEL: str = "INFO"
    DB_STATEMENT_TIMEOUT_MS: int = 30000

    @property
    def database_dsn(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
