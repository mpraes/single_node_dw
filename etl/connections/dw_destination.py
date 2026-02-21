"""PostgreSQL Data Warehouse destination engine and health-check helpers."""

from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ._config import load_connection_config
from ._engine_cache import get_or_create_engine


def _build_postgres_url(config: dict[str, str | int]) -> str:
	"""Build a SQLAlchemy PostgreSQL URL from normalized configuration values."""
	username = quote_plus(str(config["username"]))
	password = quote_plus(str(config["password"]))
	host = config["host"]
	port = config["port"]
	database = quote_plus(str(config["database"]))
	return f"postgresql+psycopg://{username}:{password}@{host}:{port}/{database}"


def get_dw_engine(
	host: str | None = None,
	port: int = 5432,
	database: str | None = None,
	username: str | None = None,
	password: str | None = None,
	*,
	config: dict | None = None,
	file_path: str | None = None,
	env_prefix: str = "DW",
	reuse: bool = True,
) -> Engine:
	"""Create or reuse the destination DW SQLAlchemy engine."""
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

	def factory() -> Engine:
		return create_engine(
			_build_postgres_url(merged_config),
			pool_pre_ping=True,
			pool_size=5,
			max_overflow=10,
		)

	return get_or_create_engine("dw_postgres", merged_config, factory, reuse=reuse)


def test_dw_connection(*args, raise_on_error: bool = False, **kwargs) -> bool:
	"""Run a lightweight SELECT 1 against the destination DW connection."""
	try:
		engine = get_dw_engine(*args, **kwargs)
		with engine.connect() as connection:
			connection.execute(text("SELECT 1"))
		return True
	except Exception:
		if raise_on_error:
			raise
		return False
