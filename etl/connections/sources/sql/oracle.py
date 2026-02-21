from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..._config import load_connection_config
from ..._engine_cache import get_or_create_engine
from .config import OracleConfig


def _build_oracle_url(config: OracleConfig) -> str:
    username = quote_plus(config.username)
    password = quote_plus(config.password)
    host = config.host
    port = config.port
    service_name = quote_plus(config.service_name)
    return f"oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}"


def get_oracle_engine(
    host: str | None = None,
    port: int = 1521,
    service_name: str | None = None,
    username: str | None = None,
    password: str | None = None,
    *,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "ORACLE",
    reuse: bool = True,
) -> Engine:
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("host", "service_name", "username", "password"),
        defaults={"port": port},
        overrides={
            "host": host,
            "port": port,
            "service_name": service_name,
            "username": username,
            "password": password,
        },
    )

    validated_config = OracleConfig.model_validate(merged_config)

    def factory() -> Engine:
        return create_engine(
            _build_oracle_url(validated_config),
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

    return get_or_create_engine("oracle", validated_config.model_dump(), factory, reuse=reuse)


def test_oracle_connection(*args, raise_on_error: bool = False, **kwargs) -> bool:
    try:
        engine = get_oracle_engine(*args, **kwargs)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 FROM dual"))
        return True
    except Exception:
        if raise_on_error:
            raise
        return False
