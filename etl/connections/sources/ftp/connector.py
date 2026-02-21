from ftplib import FTP
from pathlib import Path

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestedItem, IngestionResult
from .config import FTPConfig


class FTPConnector(BaseConnector):
    def __init__(
        self,
        host: str,
        port: int = 21,
        username: str = "anonymous",
        password: str = "",
        passive_mode: bool = True,
        remote_base_path: str = "/",
        lake_path: str = "./lake",
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "FTP",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("host", "username", "password"),
            defaults={
                "port": port,
                "passive_mode": passive_mode,
                "remote_base_path": remote_base_path,
                "lake_path": lake_path,
            },
            overrides={
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "passive_mode": passive_mode,
                "remote_base_path": remote_base_path,
                "lake_path": lake_path,
            },
        )
        self.config = FTPConfig.model_validate(merged_config)
        self.logger = get_logger("sources.ftp.connector")
        self._client: FTP | None = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting FTP connector with config=%s", safe_config)

        client = FTP()
        client.connect(host=self.config.host, port=self.config.port)
        client.login(user=self.config.username, passwd=self.config.password)
        client.set_pasv(self.config.passive_mode)
        self._client = client
        self.logger.info("FTP connector connected")

    def fetch_data(self, query: str) -> IngestionResult:
        if self._client is None:
            raise RuntimeError("FTP connector is not connected. Call connect() first.")

        remote_path = self._resolve_remote_path(query)
        lake_root = Path(self.config.lake_path)
        lake_root.mkdir(parents=True, exist_ok=True)

        items: list[IngestedItem] = []
        self.logger.info("FTP listing remote path=%s", remote_path)
        file_names = self._client.nlst(remote_path)

        for file_name in file_names:
            file_basename = Path(file_name).name
            local_path = lake_root / file_basename
            self.logger.info("FTP downloading %s to %s", file_name, local_path)

            with local_path.open("wb") as file_handle:
                self._client.retrbinary(f"RETR {file_name}", file_handle.write)

            items.append(
                IngestedItem(
                    source_path=file_name,
                    lake_path=str(local_path),
                    size_bytes=local_path.stat().st_size,
                )
            )

        return IngestionResult(
            protocol="ftp",
            success=True,
            items=items,
            metadata={"remote_path": remote_path, "downloaded_files": len(items)},
        )

    def close(self) -> None:
        self.logger.info("Closing FTP connector")
        if self._client is not None:
            try:
                self._client.quit()
            finally:
                self._client = None

    def _resolve_remote_path(self, query: str) -> str:
        raw_value = query.strip()
        if raw_value:
            return raw_value
        return self.config.remote_base_path
