from urllib.parse import urljoin

import requests

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestedItem, IngestionResult
from .config import HTTPConfig


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
        self.logger = get_logger("sources.http.connector")
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
