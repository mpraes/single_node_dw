from __future__ import annotations

import base64
from pathlib import Path
from tempfile import TemporaryDirectory

from webdav3.client import Client

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestedItem, IngestionResult
from .config import WebDAVConfig


class WebDAVConnector(BaseConnector):
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "WEBDAV",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("base_url", "username", "password"),
            overrides={
                "base_url": base_url,
                "username": username,
                "password": password,
            },
        )
        self.config = WebDAVConfig.model_validate(merged_config)
        self.logger = get_logger("sources.ftp.webdav_connector")
        self._client: Client | None = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting WebDAV connector with config=%s", safe_config)

        client = Client(
            {
                "webdav_hostname": str(self.config.base_url),
                "webdav_login": self.config.username,
                "webdav_password": self.config.password,
            }
        )

        if not client.check("/"):
            raise ConnectionError("Failed to validate WebDAV connection using '/'.")

        self._client = client
        self.logger.info("WebDAV connector connected")

    def fetch_data(self, query: str) -> IngestionResult:
        if self._client is None:
            raise RuntimeError("WebDAV connector is not connected. Call connect() first.")

        remote_path = self._resolve_remote_path(query)
        self.logger.info("WebDAV fetching remote path=%s", remote_path)

        if self._client.is_dir(remote_path):
            entries = self._client.list(remote_path=remote_path, get_info=True)
            payload = {"entries": entries}
            item = IngestedItem(source_path=remote_path, payload=payload)
            return IngestionResult(
                protocol="webdav",
                success=True,
                items=[item],
                metadata={"remote_path": remote_path, "item_type": "directory", "listed_entries": len(entries)},
            )

        file_info = self._client.info(remote_path)
        with TemporaryDirectory() as temp_dir:
            local_file = Path(temp_dir) / Path(remote_path).name
            self._client.download_sync(remote_path, str(local_file))
            file_bytes = local_file.read_bytes()

        payload, content_encoding = self._build_payload(file_bytes)
        item = IngestedItem(
            source_path=remote_path,
            size_bytes=len(file_bytes),
            payload=payload,
        )

        metadata: dict[str, str | int | float | bool | None] = {
            "remote_path": remote_path,
            "item_type": "file",
            "content_encoding": content_encoding,
            "size_bytes": len(file_bytes),
        }
        if isinstance(file_info, dict):
            metadata["modified"] = str(file_info.get("modified")) if file_info.get("modified") is not None else None
            metadata["etag"] = str(file_info.get("etag")) if file_info.get("etag") is not None else None
            metadata["content_type"] = str(file_info.get("content_type")) if file_info.get("content_type") is not None else None

        return IngestionResult(protocol="webdav", success=True, items=[item], metadata=metadata)

    def close(self) -> None:
        self.logger.info("Closing WebDAV connector")
        self._client = None

    def _resolve_remote_path(self, query: str) -> str:
        remote_path = query.strip()
        if not remote_path:
            raise ValueError("query cannot be empty. Expected WebDAV file or directory path.")
        return remote_path

    def _build_payload(self, file_bytes: bytes) -> tuple[str, str]:
        try:
            return file_bytes.decode("utf-8"), "utf-8"
        except UnicodeDecodeError:
            encoded = base64.b64encode(file_bytes).decode("ascii")
            return encoded, "base64"


__all__ = ["WebDAVConnector"]