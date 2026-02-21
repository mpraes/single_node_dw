"""Thread-safe session cache with thread-local keys for reusable clients."""

from threading import Lock, get_ident
from typing import Any, Callable

import requests

from ._logging import get_logger

_CACHE_LOCK = Lock()
_SESSION_CACHE: dict[tuple[str, tuple[tuple[str, str], ...], int], requests.Session] = {}
LOGGER = get_logger("session_cache")


def _cache_key(connection_type: str, config: dict[str, Any]) -> tuple[str, tuple[tuple[str, str], ...], int]:
    """Build a cache key that includes the current thread id."""
    normalized_items = tuple(sorted((str(key), str(value)) for key, value in config.items()))
    thread_id = get_ident()
    return connection_type, normalized_items, thread_id


def get_or_create_session(
    connection_type: str,
    config: dict[str, Any],
    factory: Callable[[], requests.Session],
    *,
    reuse: bool,
) -> requests.Session:
    """Return cached session or create/store a new one when reuse is enabled."""
    if not reuse:
        LOGGER.info("Session reuse disabled for %s, creating new session", connection_type)
        return factory()

    key = _cache_key(connection_type, config)

    with _CACHE_LOCK:
        cached = _SESSION_CACHE.get(key)
        if cached is not None:
            LOGGER.info("Session cache hit for %s on thread %s", connection_type, key[2])
            return cached

        session = factory()
        _SESSION_CACHE[key] = session
        LOGGER.info("Session cache miss for %s on thread %s, new session created", connection_type, key[2])
        return session


def close_all_sessions() -> None:
    """Close and clear all cached sessions."""
    with _CACHE_LOCK:
        sessions = list(_SESSION_CACHE.values())
        _SESSION_CACHE.clear()

    for session in sessions:
        session.close()

    LOGGER.info("Closed %s cached REST sessions", len(sessions))
