from typing import Any

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestedItem, IngestionResult
from .config import SOAPConfig


class SOAPConnector(BaseConnector):
    def __init__(
        self,
        wsdl_url: str,
        username: str | None = None,
        password: str | None = None,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "SOAP",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("wsdl_url",),
            overrides={
                "wsdl_url": wsdl_url,
                "username": username,
                "password": password,
            },
        )
        self.config = SOAPConfig.model_validate(merged_config)
        self.logger = get_logger("sources.http.soap_connector")
        self._client = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting SOAP connector with config=%s", safe_config)

        try:
            from requests import Session
            from requests.auth import HTTPBasicAuth
            from zeep import Client
            from zeep.transports import Transport
        except ImportError as exc:
            raise RuntimeError("zeep is not installed. Add it to requirements to use SOAP connector.") from exc

        session = Session()

        has_username = bool(self.config.username)
        has_password = bool(self.config.password)
        if has_username != has_password:
            raise ValueError("Provide both username and password for SOAP basic authentication.")

        if has_username and has_password:
            session.auth = HTTPBasicAuth(self.config.username, self.config.password)

        transport = Transport(session=session)
        self._client = Client(wsdl=str(self.config.wsdl_url), transport=transport)
        self.logger.info("SOAP connector connected")

    def fetch_data(self, query: str) -> IngestionResult:
        if self._client is None:
            raise RuntimeError("SOAP connector is not connected. Call connect() first.")

        method_name = query.strip()
        if not method_name:
            raise ValueError("query cannot be empty. Expected SOAP method name.")

        self.logger.info("SOAP invoking method=%s", method_name)

        try:
            soap_method = getattr(self._client.service, method_name)
        except AttributeError as exc:
            raise ValueError(f"SOAP method '{method_name}' not found in WSDL service.") from exc

        zeep_response = soap_method()
        payload = self._serialize_zeep_response(zeep_response)

        return IngestionResult(
            protocol="soap",
            success=True,
            items=[
                IngestedItem(
                    source_path=method_name,
                    payload={"result": payload},
                )
            ],
            metadata={
                "wsdl_url": str(self.config.wsdl_url),
                "method": method_name,
            },
        )

    def close(self) -> None:
        self.logger.info("Closing SOAP connector")
        self._client = None

    def _serialize_zeep_response(self, response: Any) -> dict | list | str | int | float | bool | None:
        try:
            from zeep.helpers import serialize_object
        except ImportError as exc:
            raise RuntimeError("zeep is not installed. Add it to requirements to use SOAP connector.") from exc

        return serialize_object(response)