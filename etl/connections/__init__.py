"""Public entrypoints for connection builders, tests, and cache cleanup."""

from typing import Any

from sqlalchemy.engine import Engine

from ._engine_cache import dispose_all_engines
from ._session_cache import close_all_sessions
from .dw_destination import get_dw_engine, test_dw_connection
from .sources.http import get_rest_session, request_rest, test_rest_connection
from .sources.nosql.cassandra import get_cassandra_session, test_cassandra_connection
from .sources.nosql.neo4j import get_neo4j_driver, test_neo4j_connection
from .sources.factory import create_connector, load_connector_config
from .sources.sql.mssql import get_mssql_engine, test_mssql_connection
from .sources.sql.oracle import get_oracle_engine, test_oracle_connection
from .sources.sql.postgres import get_postgres_engine, test_postgres_connection
from .sources.sql.sqlite import get_sqlite_engine, test_sqlite_connection


def get_connection(source: str, **kwargs) -> Any:
    """Return a connection client/engine for the requested source alias."""
    source_key = source.strip().lower()

    if source_key in {"dw", "postgres", "destination", "dw_destination"}:
        return get_dw_engine(**kwargs)

    if source_key in {"mssql", "sqlserver", "sql_server"}:
        return get_mssql_engine(**kwargs)

    if source_key in {"oracle", "oracle_db"}:
        return get_oracle_engine(**kwargs)

    if source_key in {"postgres", "pgsql", "postgresql", "source_postgres"}:
        return get_postgres_engine(**kwargs)

    if source_key in {"sqlite", "sqlite3"}:
        return get_sqlite_engine(**kwargs)

    if source_key in {"cassandra"}:
        return get_cassandra_session(**kwargs)

    if source_key in {"neo4j", "bolt"}:
        return get_neo4j_driver(**kwargs)

    if source_key in {"rest", "api", "http"}:
        return get_rest_session(**kwargs)

    raise ValueError(f"Unsupported source '{source}'. Use one of: dw, mssql, oracle, rest")


def test_connection(source: str, **kwargs) -> bool:
    """Run a lightweight connection health check for the requested source alias."""
    source_key = source.strip().lower()

    if source_key in {"dw", "postgres", "destination", "dw_destination"}:
        return test_dw_connection(**kwargs)

    if source_key in {"mssql", "sqlserver", "sql_server"}:
        return test_mssql_connection(**kwargs)

    if source_key in {"oracle", "oracle_db"}:
        return test_oracle_connection(**kwargs)

    if source_key in {"postgres", "pgsql", "postgresql", "source_postgres"}:
        return test_postgres_connection(**kwargs)

    if source_key in {"sqlite", "sqlite3"}:
        return test_sqlite_connection(**kwargs)

    if source_key in {"cassandra"}:
        return test_cassandra_connection(**kwargs)

    if source_key in {"neo4j", "bolt"}:
        return test_neo4j_connection(**kwargs)

    if source_key in {"rest", "api", "http"}:
        return test_rest_connection(**kwargs)

    raise ValueError(f"Unsupported source '{source}'. Use one of: dw, mssql, oracle, rest")


def close_all_connections() -> None:
    """Dispose all cached engines and close all cached sessions."""
    dispose_all_engines()
    close_all_sessions()


__all__ = [
    "get_connection",
    "test_connection",
    "create_connector",
    "load_connector_config",
    "close_all_connections",
    "dispose_all_engines",
    "close_all_sessions",
    "get_dw_engine",
    "test_dw_connection",
    "get_mssql_engine",
    "test_mssql_connection",
    "get_oracle_engine",
    "test_oracle_connection",
    "get_postgres_engine",
    "test_postgres_connection",
    "get_sqlite_engine",
    "test_sqlite_connection",
    "get_cassandra_session",
    "test_cassandra_connection",
    "get_neo4j_driver",
    "test_neo4j_connection",
    "get_rest_session",
    "request_rest",
    "test_rest_connection",
]