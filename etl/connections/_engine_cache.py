"""Thread-safe SQLAlchemy engine cache keyed by connection config."""

from threading import Lock
from typing import Any, Callable

from sqlalchemy.engine import Engine

from ._logging import get_logger

_CACHE_LOCK = Lock()
_ENGINE_CACHE: dict[tuple[str, tuple[tuple[str, str], ...]], Engine] = {}
LOGGER = get_logger("engine_cache")


def _cache_key(connection_type: str, config: dict[str, Any]) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Build a stable cache key from connection type and normalized config values."""
    normalized_items = tuple(sorted((str(key), str(value)) for key, value in config.items()))
    return connection_type, normalized_items


def get_or_create_engine(
    connection_type: str,
    config: dict[str, Any],
    factory: Callable[[], Engine],
    *,
    reuse: bool,
) -> Engine:
    """Return cached engine or create/store a new one when reuse is enabled."""
    if not reuse:
        LOGGER.info("Engine reuse disabled for %s, creating new engine", connection_type)
        return factory()

    key = _cache_key(connection_type, config)

    with _CACHE_LOCK:
        cached = _ENGINE_CACHE.get(key)
        if cached is not None:
            LOGGER.info("Engine cache hit for %s", connection_type)
            return cached

        engine = factory()
        _ENGINE_CACHE[key] = engine
        LOGGER.info("Engine cache miss for %s, new engine created", connection_type)
        return engine


def dispose_all_engines() -> None:
    """Dispose and clear all cached SQLAlchemy engines."""
    with _CACHE_LOCK:
        engines = list(_ENGINE_CACHE.values())
        _ENGINE_CACHE.clear()

    for engine in engines:
        engine.dispose()

    LOGGER.info("Disposed %s cached SQL engines", len(engines))