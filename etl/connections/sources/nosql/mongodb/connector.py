from datetime import date, datetime
from decimal import Decimal
from typing import Any

from bson import Binary, Decimal128, ObjectId
from pymongo import MongoClient

from ...._config import load_connection_config
from ...._logging import get_logger, redact_config
from ...base_connector import BaseConnector
from ...data_contract import IngestedItem, IngestionResult
from .config import MongoDBConfig


class MongoDBConnector(BaseConnector):
    def __init__(
        self,
        host: str,
        port: int = 27017,
        username: str | None = None,
        password: str | None = None,
        database: str | None = None,
        auth_source: str | None = None,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "MONGODB",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("host", "database"),
            defaults={"port": port},
            overrides={
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "database": database,
                "auth_source": auth_source,
            },
        )
        self.config = MongoDBConfig.model_validate(merged_config)
        self.logger = get_logger("sources.nosql.mongodb.connector")
        self._client: MongoClient | None = None
        self._database = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting MongoDB connector with config=%s", safe_config)

        client_kwargs: dict[str, Any] = {
            "host": self.config.host,
            "port": self.config.port,
        }

        if self.config.username:
            client_kwargs["username"] = self.config.username
        if self.config.password:
            client_kwargs["password"] = self.config.password
        if self.config.auth_source:
            client_kwargs["authSource"] = self.config.auth_source

        client = MongoClient(**client_kwargs)
        self._client = client
        self._database = client[self.config.database]
        self.logger.info("MongoDB connector connected")

    def fetch_data(self, query: str) -> IngestionResult:
        if self._database is None:
            raise RuntimeError("MongoDB connector is not connected. Call connect() first.")

        collection_name = query.strip()
        if not collection_name:
            raise ValueError("query cannot be empty. Expected collection name.")

        self.logger.info("MongoDB fetching collection=%s", collection_name)
        collection = self._database[collection_name]
        documents = list(collection.find())

        items = [
            IngestedItem(payload=self._to_serializable(document))
            for document in documents
        ]

        return IngestionResult(
            protocol="mongodb",
            success=True,
            items=items,
            metadata={"collection": collection_name, "fetched_documents": len(items)},
        )

    def close(self) -> None:
        self.logger.info("Closing MongoDB connector")
        if self._client is not None:
            self._client.close()
            self._client = None
            self._database = None

    def _to_serializable(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): self._to_serializable(item) for key, item in value.items()}

        if isinstance(value, list):
            return [self._to_serializable(item) for item in value]

        if isinstance(value, tuple):
            return [self._to_serializable(item) for item in value]

        if isinstance(value, (ObjectId, Decimal128, Binary, bytes, bytearray, datetime, date, Decimal)):
            return str(value)

        if isinstance(value, (str, int, float, bool)) or value is None:
            return value

        return str(value)
