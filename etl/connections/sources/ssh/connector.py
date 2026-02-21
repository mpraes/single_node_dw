from pathlib import Path

import paramiko

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestedItem, IngestionResult
from .config import SSHConfig


class SSHConnector(BaseConnector):
    def __init__(
        self,
        host: str,
        port: int = 22,
        username: str = "",
        password: str | None = None,
        private_key_path: str | None = None,
        remote_base_path: str = ".",
        lake_path: str = "./lake",
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "SSH",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("host", "username"),
            defaults={
                "port": port,
                "remote_base_path": remote_base_path,
                "lake_path": lake_path,
            },
            overrides={
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "private_key_path": private_key_path,
                "remote_base_path": remote_base_path,
                "lake_path": lake_path,
            },
        )
        self.config = SSHConfig.model_validate(merged_config)
        self.logger = get_logger("sources.ssh.connector")
        self._client: paramiko.SSHClient | None = None
        self._sftp = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting SSH connector with config=%s", safe_config)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {
            "hostname": self.config.host,
            "port": self.config.port,
            "username": self.config.username,
        }

        if self.config.private_key_path:
            private_key = paramiko.RSAKey.from_private_key_file(self.config.private_key_path)
            connect_kwargs["pkey"] = private_key
        else:
            connect_kwargs["password"] = self.config.password

        client.connect(**connect_kwargs)
        self._client = client
        self._sftp = client.open_sftp()
        self.logger.info("SSH connector connected")

    def fetch_data(self, query: str) -> IngestionResult:
        if self._sftp is None:
            raise RuntimeError("SSH connector is not connected. Call connect() first.")

        remote_path = self._resolve_remote_path(query)
        lake_root = Path(self.config.lake_path)
        lake_root.mkdir(parents=True, exist_ok=True)

        self.logger.info("SSH listing remote path=%s", remote_path)
        remote_items = self._sftp.listdir_attr(remote_path)
        ingested_items: list[IngestedItem] = []

        for remote_item in remote_items:
            remote_name = remote_item.filename
            remote_file = f"{remote_path.rstrip('/')}/{remote_name}"
            local_file = lake_root / remote_name

            self.logger.info("SSH downloading %s to %s", remote_file, local_file)
            self._sftp.get(remote_file, str(local_file))

            ingested_items.append(
                IngestedItem(
                    source_path=remote_file,
                    lake_path=str(local_file),
                    size_bytes=remote_item.st_size,
                )
            )

        return IngestionResult(
            protocol="ssh",
            success=True,
            items=ingested_items,
            metadata={"remote_path": remote_path, "downloaded_files": len(ingested_items)},
        )

    def close(self) -> None:
        self.logger.info("Closing SSH connector")
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None

        if self._client is not None:
            self._client.close()
            self._client = None

    def _resolve_remote_path(self, query: str) -> str:
        raw_value = query.strip()
        if raw_value:
            return raw_value
        return self.config.remote_base_path
