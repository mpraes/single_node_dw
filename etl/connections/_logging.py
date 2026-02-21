import logging
import os
from threading import Lock
from typing import Any

_SETUP_LOCK = Lock()
_SETUP_DONE = False
_SENSITIVE_KEYS = {
    "password",
    "token",
    "secret",
    "api_key",
    "apikey",
    "authorization",
}


def _resolve_log_level() -> int:
    level_name = os.getenv("ETL_LOG_LEVEL", "INFO").upper()
    return getattr(logging, level_name, logging.INFO)


def _setup_default_logging() -> None:
    global _SETUP_DONE
    if _SETUP_DONE:
        return

    with _SETUP_LOCK:
        if _SETUP_DONE:
            return

        root_logger = logging.getLogger()
        if not root_logger.handlers:
            logging.basicConfig(
                level=_resolve_log_level(),
                format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            )

        _SETUP_DONE = True


def get_logger(name: str) -> logging.Logger:
    _setup_default_logging()
    return logging.getLogger(f"etl.connections.{name}")


def redact_config(values: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in values.items():
        if key.lower() in _SENSITIVE_KEYS and value is not None:
            redacted[key] = "***"
        else:
            redacted[key] = value
    return redacted