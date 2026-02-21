from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..._config import load_connection_config
from ..._engine_cache import get_or_create_engine
from .config import MSSQLConfig


def _build_mssql_url(config: MSSQLConfig) -> str:
    username = quote_plus(config.username)
    password = quote_plus(config.password)
    host = config.host
    port = config.port
    database = quote_plus(config.database)
    driver = quote_plus(config.driver)
    trust_server_certificate = config.trust_server_certificate
    return (
        f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?"
        f"driver={driver}&TrustServerCertificate={trust_server_certificate}"
    )


def get_mssql_engine(
    host: str | None = None,
    port: int = 1433,
    database: str | None = None,
    username: str | None = None,
    password: str | None = None,
    driver: str = "ODBC Driver 18 for SQL Server",
    trust_server_certificate: str = "yes",
    *,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "MSSQL",
    reuse: bool = True,
) -> Engine:
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("host", "database", "username", "password"),
        defaults={
            "port": port,
            "driver": driver,
            "trust_server_certificate": trust_server_certificate,
        },
        overrides={
            "host": host,
            "port": port,
            "database": database,
            "username": username,
            "password": password,
            "driver": driver,
            "trust_server_certificate": trust_server_certificate,
        },
    )

    validated_config = MSSQLConfig.model_validate(merged_config)

    def factory() -> Engine:
        return create_engine(
            _build_mssql_url(validated_config),
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

    return get_or_create_engine("mssql", validated_config.model_dump(), factory, reuse=reuse)


def test_mssql_connection(*args, raise_on_error: bool = False, **kwargs) -> bool:
    try:
        engine = get_mssql_engine(*args, **kwargs)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception:
        if raise_on_error:
            raise
        return False
