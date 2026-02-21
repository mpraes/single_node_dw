from __future__ import annotations

"""Dynamic connector factory based on protocol-driven module discovery."""

import importlib
import inspect
import json
from pathlib import Path
from pkgutil import walk_packages
from typing import Any

from .._logging import get_logger, redact_config
from .base_connector import BaseConnector

logger = get_logger("sources.factory")
_SOURCES_PACKAGE = "connections.sources"


def load_connector_config(config: dict[str, Any] | str | Path) -> dict[str, Any]:
    """Load connector config from dict, JSON file, or YAML file."""
    if isinstance(config, dict):
        return config

    config_path = Path(config)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    suffix = config_path.suffix.lower()
    content = config_path.read_text(encoding="utf-8")

    if suffix == ".json":
        data = json.loads(content)
    elif suffix in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise RuntimeError("PyYAML is not installed. Add it to requirements to use YAML config files.") from exc

        data = yaml.safe_load(content)
    else:
        raise ValueError("Unsupported config format. Use JSON (.json) or YAML (.yaml/.yml).")

    if not isinstance(data, dict):
        raise ValueError("Connector configuration must be a key-value object.")

    return data


def create_connector(config: dict[str, Any] | str | Path) -> BaseConnector:
    """Instantiate connector class inferred from the protocol field."""
    resolved_config = load_connector_config(config)
    protocol = _normalize_protocol(resolved_config.get("protocol"))

    payload = dict(resolved_config)
    payload.pop("protocol", None)

    connector_class = _resolve_connector_class(protocol)
    safe_payload = redact_config(payload)

    logger.info("Creating connector protocol=%s class=%s config=%s", protocol, connector_class.__name__, safe_payload)

    try:
        connector = connector_class(**payload)
    except TypeError as exc:
        raise TypeError(
            f"Invalid parameters for protocol '{protocol}' using connector '{connector_class.__name__}': {exc}"
        ) from exc

    return connector


def _normalize_protocol(value: Any) -> str:
    """Validate and normalize protocol name to lowercase string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing required 'protocol' field in connector configuration.")
    return value.strip().lower()


def _resolve_connector_class(protocol: str) -> type[BaseConnector]:
    """Resolve connector class by scanning protocol candidate modules."""
    for module_name in _iter_candidate_modules(protocol):
        connector_class = _find_connector_class(module_name, protocol)
        if connector_class is not None:
            return connector_class

    raise ValueError(
        f"Unsupported protocol '{protocol}'. Add a connector module/class under '{_SOURCES_PACKAGE}' following existing patterns."
    )


def _iter_candidate_modules(protocol: str) -> list[str]:
    """Build candidate module names for protocol and nested connector modules."""
    candidates: set[str] = {
        f"{_SOURCES_PACKAGE}.{protocol}",
        f"{_SOURCES_PACKAGE}.{protocol}.connector",
        f"{_SOURCES_PACKAGE}.{protocol}_connector",
    }

    base_package = importlib.import_module(_SOURCES_PACKAGE)

    for module_info in walk_packages(base_package.__path__, prefix=f"{_SOURCES_PACKAGE}."):
        module_name = module_info.name
        if module_name.endswith(f".{protocol}"):
            candidates.add(module_name)
        elif module_name.endswith(f".{protocol}.connector"):
            candidates.add(module_name)
        elif module_name.endswith(f".{protocol}_connector"):
            candidates.add(module_name)

    return sorted(candidates)


def _find_connector_class(module_name: str, protocol: str) -> type[BaseConnector] | None:
    """Return preferred or first valid connector class from module."""
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        return None

    preferred_class_name = f"{protocol.upper()}Connector"
    fallback: type[BaseConnector] | None = None

    for _, member in inspect.getmembers(module, inspect.isclass):
        if not issubclass(member, BaseConnector) or member is BaseConnector:
            continue
        if member.__module__ != module.__name__:
            continue

        if member.__name__.lower() == preferred_class_name.lower():
            return member

        if fallback is None:
            fallback = member

    return fallback


__all__ = ["load_connector_config", "create_connector"]
