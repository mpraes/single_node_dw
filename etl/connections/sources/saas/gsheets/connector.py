from typing import Any

from ...._config import load_connection_config
from ...._logging import get_logger, redact_config
from ...base_connector import BaseConnector
from ...data_contract import IngestedItem, IngestionResult
from .config import GSheetsConfig


class GSheetsConnector(BaseConnector):
    def __init__(
        self,
        service_account_info: dict[str, Any],
        spreadsheet_id: str,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "GSHEETS",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("service_account_info", "spreadsheet_id"),
            overrides={
                "service_account_info": service_account_info,
                "spreadsheet_id": spreadsheet_id,
            },
        )
        self.config = GSheetsConfig.model_validate(merged_config)
        self.logger = get_logger("sources.saas.gsheets.connector")
        self._client = None
        self._spreadsheet = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting GSheets connector with config=%s", safe_config)

        try:
            from gspread import service_account_from_dict
        except ImportError as exc:
            raise RuntimeError("gspread is not installed. Add it to requirements to use Google Sheets connector.") from exc

        client = service_account_from_dict(self.config.service_account_info)
        spreadsheet = client.open_by_key(self.config.spreadsheet_id)

        self._client = client
        self._spreadsheet = spreadsheet
        self.logger.info("GSheets connector connected")

    def fetch_data(self, query: str) -> IngestionResult:
        if self._spreadsheet is None:
            raise RuntimeError("GSheets connector is not connected. Call connect() first.")

        worksheet_name = query.strip()
        if not worksheet_name:
            raise ValueError("query cannot be empty. Expected worksheet name.")

        self.logger.info("GSheets loading worksheet=%s", worksheet_name)
        worksheet = self._spreadsheet.worksheet(worksheet_name)
        records = worksheet.get_all_records()

        return IngestionResult(
            protocol="gsheets",
            success=True,
            items=[
                IngestedItem(
                    source_path=worksheet_name,
                    payload=records,
                )
            ],
            metadata={
                "spreadsheet_id": self.config.spreadsheet_id,
                "worksheet": worksheet_name,
                "rows": len(records),
            },
        )

    def close(self) -> None:
        self.logger.info("Closing GSheets connector")
        self._spreadsheet = None
        self._client = None
