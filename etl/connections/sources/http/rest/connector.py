from urllib.parse import urljoin

import requests

from ...._config import load_connection_config
from ...._logging import get_logger, redact_config
from ...._session_cache import get_or_create_session
from ...base_connector import BaseConnector
from ...data_contract import IngestedItem, IngestionResult
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


class HTTPConnector(BaseConnector):
    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        timeout_seconds: int = 30,
        client_library: str = "requests",
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "REST",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("base_url",),
            defaults={
                "timeout_seconds": timeout_seconds,
                "client_library": client_library,
            },
            overrides={
                "base_url": base_url,
                "token": token,
                "timeout_seconds": timeout_seconds,
                "client_library": client_library,
            },
        )
        self.config = HTTPConfig.model_validate(merged_config)
        self.logger = get_logger("sources.http.rest.connector")
        self._requests_session: requests.Session | None = None
        self._httpx_client = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting HTTP connector with config=%s", safe_config)

        if self.config.client_library == "requests":
            session = requests.Session()
            session.headers.update(self._build_default_headers())
            self._requests_session = session
            self.logger.info("HTTP connector ready using requests")
            return

        try:
            import httpx  # type: ignore
        except ImportError as exc:
            raise RuntimeError("httpx is not installed. Add it to requirements to use client_library='httpx'.") from exc

        self._httpx_client = httpx.Client(
            headers=self._build_default_headers(),
            timeout=self.config.timeout_seconds,
        )
        self.logger.info("HTTP connector ready using httpx")

    def fetch_data(self, query: str) -> IngestionResult:
        if not query.strip():
            raise ValueError("query cannot be empty. Expected endpoint path like '/health'.")

        if self.config.client_library == "requests":
            if self._requests_session is None:
                raise RuntimeError("HTTP connector is not connected. Call connect() first.")

            response = self._requests_session.get(
                urljoin(str(self.config.base_url).rstrip("/") + "/", query.lstrip("/")),
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            payload = self._response_payload(response.text, response.headers.get("Content-Type", ""))
            return IngestionResult(
                protocol="http",
                success=True,
                items=[IngestedItem(payload=payload)],
                metadata={"status_code": response.status_code, "client_library": "requests"},
            )

        if self._httpx_client is None:
            raise RuntimeError("HTTP connector is not connected. Call connect() first.")

        response = self._httpx_client.get(urljoin(str(self.config.base_url).rstrip("/") + "/", query.lstrip("/")))
        response.raise_for_status()
        payload = self._response_payload(response.text, response.headers.get("Content-Type", ""))
        return IngestionResult(
            protocol="http",
            success=True,
            items=[IngestedItem(payload=payload)],
            metadata={"status_code": response.status_code, "client_library": "httpx"},
        )

    def close(self) -> None:
        self.logger.info("Closing HTTP connector")

        if self._requests_session is not None:
            self._requests_session.close()
            self._requests_session = None

        if self._httpx_client is not None:
            self._httpx_client.close()
            self._httpx_client = None

    def _build_default_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        return headers

    def _response_payload(self, response_text: str, content_type: str) -> dict | list | str:
        if "application/json" in content_type.lower():
            try:
                import json

                return json.loads(response_text)
            except Exception:
                self.logger.warning("Failed to decode JSON payload, returning raw text")
        return response_text


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
