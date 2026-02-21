from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..._session_cache import get_or_create_session
from .config import CassandraConfig

LOGGER = get_logger("sources.nosql.cassandra")


def _normalize_hosts(hosts: str | list[str] | None) -> list[str]:
    if hosts is None:
        return []
    if isinstance(hosts, str):
        return [item.strip() for item in hosts.split(",") if item.strip()]
    return [item.strip() for item in hosts if item and item.strip()]


def get_cassandra_session(
    hosts: str | list[str] | None = None,
    port: int = 9042,
    keyspace: str | None = None,
    username: str | None = None,
    password: str | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "CASSANDRA",
    reuse: bool = True,
) -> Session:
    normalized_hosts = _normalize_hosts(hosts)
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("hosts", "keyspace"),
        defaults={"port": port},
        overrides={
            "hosts": normalized_hosts if normalized_hosts else None,
            "port": port,
            "keyspace": keyspace,
            "username": username,
            "password": password,
        },
    )

    validated_config = CassandraConfig.model_validate(merged_config)
    LOGGER.info("Creating Cassandra session with config=%s", redact_config(validated_config.model_dump()))

    cache_config = {
        "hosts": ",".join(validated_config.hosts),
        "port": str(validated_config.port),
        "keyspace": validated_config.keyspace,
        "username": validated_config.username or "",
    }

    def factory() -> Session:
        auth_provider = None
        if validated_config.username and validated_config.password:
            auth_provider = PlainTextAuthProvider(validated_config.username, validated_config.password)
        cluster = Cluster(
            contact_points=validated_config.hosts,
            port=validated_config.port,
            auth_provider=auth_provider,
        )
        session = cluster.connect(validated_config.keyspace)
        session._etl_cluster = cluster
        return session

    return get_or_create_session("cassandra", cache_config, factory, reuse=reuse)


def close_cassandra_session(session: Session) -> None:
    cluster = getattr(session, "_etl_cluster", None)
    session.shutdown()
    if cluster is not None:
        cluster.shutdown()


def test_cassandra_connection(
    hosts: str | list[str] | None = None,
    port: int = 9042,
    keyspace: str | None = None,
    username: str | None = None,
    password: str | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "CASSANDRA",
    reuse: bool = True,
    raise_on_error: bool = False,
) -> bool:
    try:
        session = get_cassandra_session(
            hosts=hosts,
            port=port,
            keyspace=keyspace,
            username=username,
            password=password,
            config=config,
            file_path=file_path,
            env_prefix=env_prefix,
            reuse=reuse,
        )
        session.execute("SELECT release_version FROM system.local")
        return True
    except Exception:
        LOGGER.exception("Cassandra connection test failed")
        if raise_on_error:
            raise
        return False
