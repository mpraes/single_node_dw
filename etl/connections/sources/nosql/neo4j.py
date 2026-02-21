from neo4j import Driver, GraphDatabase

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..._session_cache import get_or_create_session
from .config import Neo4jConfig

LOGGER = get_logger("sources.nosql.neo4j")


def get_neo4j_driver(
    uri: str | None = None,
    username: str | None = None,
    password: str | None = None,
    database: str = "neo4j",
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "NEO4J",
    reuse: bool = True,
) -> Driver:
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("uri", "username", "password"),
        defaults={"database": database},
        overrides={
            "uri": uri,
            "username": username,
            "password": password,
            "database": database,
        },
    )
    validated_config = Neo4jConfig.model_validate(merged_config)
    LOGGER.info("Creating Neo4j driver with config=%s", redact_config(validated_config.model_dump()))

    cache_config = {
        "uri": validated_config.uri,
        "username": validated_config.username,
        "database": validated_config.database,
    }

    def factory() -> Driver:
        return GraphDatabase.driver(
            validated_config.uri,
            auth=(validated_config.username, validated_config.password),
        )

    return get_or_create_session("neo4j", cache_config, factory, reuse=reuse)


def test_neo4j_connection(
    uri: str | None = None,
    username: str | None = None,
    password: str | None = None,
    database: str = "neo4j",
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "NEO4J",
    reuse: bool = True,
    raise_on_error: bool = False,
) -> bool:
    try:
        driver = get_neo4j_driver(
            uri=uri,
            username=username,
            password=password,
            database=database,
            config=config,
            file_path=file_path,
            env_prefix=env_prefix,
            reuse=reuse,
        )
        with driver.session(database=database) as session:
            result = session.run("RETURN 1 AS ok")
            record = result.single()
            return bool(record and record.get("ok") == 1)
    except Exception:
        LOGGER.exception("Neo4j connection test failed")
        if raise_on_error:
            raise
        return False
