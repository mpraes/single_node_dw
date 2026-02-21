from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..._config import load_connection_config
from ..._engine_cache import get_or_create_engine
from ..._logging import get_logger, redact_config
from .config import PostgresConfig

LOGGER = get_logger("sources.sql.postgres")


def _build_postgres_url(config: PostgresConfig) -> str:
    username = quote_plus(config.username)
    password = quote_plus(config.password)
    host = config.host
    port = config.port
    database = quote_plus(config.database)
    return f"postgresql+psycopg://{username}:{password}@{host}:{port}/{database}"


def get_postgres_engine(
    host: str | None = None,
    port: int = 5432,
    database: str | None = None,
    username: str | None = None,
    password: str | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "PG",
    reuse: bool = True,
) -> Engine:
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("host", "database", "username", "password"),
        defaults={"port": port},
        overrides={
            "host": host,
            "port": port,
            "database": database,
            "username": username,
            "password": password,
        },
    )
    validated_config = PostgresConfig.model_validate(merged_config)
    LOGGER.info("Creating Postgres engine with config=%s", redact_config(validated_config.model_dump()))

    def factory() -> Engine:
        return create_engine(
            _build_postgres_url(validated_config),
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

    return get_or_create_engine("src_postgres", validated_config.model_dump(), factory, reuse=reuse)


def test_postgres_connection(
    host: str | None = None,
    port: int = 5432,
    database: str | None = None,
    username: str | None = None,
    password: str | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "PG",
    reuse: bool = True,
    raise_on_error: bool = False,
) -> bool:
    try:
        engine = get_postgres_engine(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            config=config,
            file_path=file_path,
            env_prefix=env_prefix,
            reuse=reuse,
        )
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception:
        LOGGER.exception("Postgres connection test failed")
        if raise_on_error:
            raise
        return False
