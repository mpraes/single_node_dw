from urllib.parse import urljoin

import requests

from ..._config import load_connection_config
from ..._session_cache import get_or_create_session
from .config import HTTPConfig


def _normalize_base_url(base_url: str) -> str:
    normalized = base_url.strip()
    if not normalized:
        raise ValueError("base_url cannot be empty")
    return normalized.rstrip("/") + "/"


def _build_auth_header(token: str | None) -> dict[str, str]:
    if not token:
        return {}
    return {"Authorization": f"Bearer {token}"}


def _build_default_headers(token: str | None) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    headers.update(_build_auth_header(token))
    return headers


def _merge_headers(base: dict[str, str], extra: dict[str, str] | None) -> dict[str, str]:
    merged = dict(base)
    if extra:
        merged.update(extra)
    return merged


def _build_request_url(base_url: str, endpoint: str) -> str:
    clean_endpoint = endpoint.lstrip("/")
    return urljoin(base_url, clean_endpoint)


def get_rest_session(
    base_url: str | None = None,
    token: str | None = None,
    timeout_seconds: int = 30,
    *,
    headers: dict[str, str] | None = None,
    config: dict | None = None,
    file_path: str | None = None,
    env_prefix: str = "REST",
    reuse: bool = True,
) -> requests.Session:
    merged_config = load_connection_config(
        config,
        file_path=file_path,
        env_prefix=env_prefix,
        required=("base_url",),
        defaults={"timeout_seconds": timeout_seconds},
        overrides={"base_url": base_url, "token": token, "timeout_seconds": timeout_seconds},
    )

    validated_config = HTTPConfig.model_validate(merged_config)
    normalized_base_url = _normalize_base_url(str(validated_config.base_url))
    default_headers = _build_default_headers(validated_config.token)
    merged_headers = _merge_headers(default_headers, headers)

    cache_config = {
        "base_url": normalized_base_url,
        "headers": str(sorted(merged_headers.items())),
        "timeout_seconds": str(validated_config.timeout_seconds),
    }

    def factory() -> requests.Session:
        session = requests.Session()
        session.headers.update(merged_headers)
        return session

    session = get_or_create_session("rest", cache_config, factory, reuse=reuse)
    session.base_url = normalized_base_url
    session.timeout_seconds = int(validated_config.timeout_seconds)
    return session


def request_rest(
    method: str,
    endpoint: str,
    *,
    params: dict | None = None,
    json_body: dict | list | None = None,
    data: dict | str | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int | None = None,
    raise_for_status: bool = True,
    **session_kwargs,
) -> requests.Response:
    session = get_rest_session(**session_kwargs)
    request_url = _build_request_url(session.base_url, endpoint)
    request_headers = _merge_headers(dict(session.headers), headers)
    request_timeout = timeout_seconds or session.timeout_seconds

    response = session.request(
        method=method.upper(),
        url=request_url,
        params=params,
        json=json_body,
        data=data,
        headers=request_headers,
        timeout=request_timeout,
    )

    if raise_for_status:
        response.raise_for_status()

    return response


def test_rest_connection(
    endpoint: str = "/health",
    expected_status: int = 200,
    *,
    method: str = "GET",
    raise_on_error: bool = False,
    **session_kwargs,
) -> bool:
    try:
        response = request_rest(
            method=method,
            endpoint=endpoint,
            raise_for_status=False,
            **session_kwargs,
        )
        return response.status_code == expected_status
    except Exception:
        if raise_on_error:
            raise
        return False
