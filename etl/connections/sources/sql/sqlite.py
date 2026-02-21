from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..._config import load_connection_config
from ..._engine_cache import get_or_create_engine
from ..._logging import get_logger, redact_config
from .config import SQLiteConfig

LOGGER = get_logger("sources.sql.sqlite")


def _build_sqlite_url(database_path: str) -> str:
    if database_path == ":memory:":
        return "sqlite+pysqlite:///:memory:"

    resolved_path = Path(database_path).expanduser().resolve()
    return f"sqlite+pysqlite:///{resolved_path}"


def get_sqlite_engine(
    database_path: str | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "SQLITE",
    reuse: bool = True,
) -> Engine:
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("database_path",),
        overrides={"database_path": database_path},
    )
    validated_config = SQLiteConfig.model_validate(merged_config)
    LOGGER.info("Creating SQLite engine with config=%s", redact_config(validated_config.model_dump()))

    def factory() -> Engine:
        return create_engine(_build_sqlite_url(validated_config.database_path), pool_pre_ping=True)

    return get_or_create_engine("sqlite", validated_config.model_dump(), factory, reuse=reuse)


def test_sqlite_connection(
    database_path: str | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "SQLITE",
    reuse: bool = True,
    raise_on_error: bool = False,
) -> bool:
    try:
        engine = get_sqlite_engine(
            database_path=database_path,
            config=config,
            file_path=file_path,
            env_prefix=env_prefix,
            reuse=reuse,
        )
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception:
        LOGGER.exception("SQLite connection test failed")
        if raise_on_error:
            raise
        return False
