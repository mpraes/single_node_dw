"""Configuration loader utilities shared by all connection modules."""

import json
import os
from pathlib import Path
from typing import Any

from ._logging import get_logger, redact_config

LOGGER = get_logger("config")


def _read_prefixed_env(prefix: str) -> dict[str, Any]:
    """Read environment keys matching <PREFIX>_* and normalize key names."""
    prefix_token = f"{prefix.upper()}_"
    values: dict[str, Any] = {}

    for key, value in os.environ.items():
        if key.startswith(prefix_token):
            normalized_key = key.removeprefix(prefix_token).lower()
            values[normalized_key] = value

    LOGGER.info("Loaded %s config keys from environment prefix %s", len(values), prefix_token)
    return values


def _validate_json_root(data: Any) -> dict[str, Any]:
    """Ensure configuration files deserialize to a dictionary root."""
    if isinstance(data, dict):
        return data
    raise ValueError("Config file must contain a JSON object at the root")


def _read_json_file(file_path: str | None) -> dict[str, Any]:
    """Read JSON config file when provided, otherwise return an empty mapping."""
    if not file_path:
        LOGGER.info("No config file path provided")
        return {}

    path = Path(file_path)
    if not path.exists():
        LOGGER.error("Config file not found: %s", file_path)
        raise FileNotFoundError(f"Config file not found: {file_path}")

    raw_data = json.loads(path.read_text(encoding="utf-8"))
    LOGGER.info("Loaded JSON config from %s", file_path)
    return _validate_json_root(raw_data)


def _not_none_values(values: dict[str, Any] | None) -> dict[str, Any]:
    """Drop keys with None values to avoid overriding previous layers."""
    if not values:
        return {}
    LOGGER.info("Normalized non-null overrides with %s keys", len(values))
    return {key: value for key, value in values.items() if value is not None}


def _merge_config_layers(layers: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge config dictionaries in order where last layer wins."""
    merged: dict[str, Any] = {}
    for layer in layers:
        merged.update(layer)
    LOGGER.info("Merged %s config layers", len(layers))
    return merged


def _ensure_required_keys(config: dict[str, Any], required: tuple[str, ...]) -> None:
    """Validate required keys and raise a clear error when missing."""
    missing = [key for key in required if config.get(key) in (None, "")]
    if missing:
        joined = ", ".join(missing)
        LOGGER.error("Required config keys missing: %s", joined)
        raise ValueError(f"Missing required connection config keys: {joined}")
    LOGGER.info("Required config keys validated: %s", ", ".join(required) if required else "none")


def load_connection_config(
    config: dict[str, Any] | None = None,
    *,
    file_path: str | None = None,
    env_prefix: str | None = None,
    required: tuple[str, ...] = (),
    defaults: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve final connector config from defaults, file, env, config, and overrides."""
    LOGGER.info(
        "Loading connection config with env_prefix=%s, file_path=%s",
        env_prefix,
        file_path,
    )
    env_config = _read_prefixed_env(env_prefix) if env_prefix else {}
    merged = _merge_config_layers(
        [
            defaults or {},
            _read_json_file(file_path),
            env_config,
            config or {},
            _not_none_values(overrides),
        ]
    )

    _ensure_required_keys(merged, required)
    LOGGER.info("Connection config resolved: %s", redact_config(merged))
    return merged