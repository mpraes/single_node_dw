from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class GSheetsConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    service_account_info: dict[str, Any]
    spreadsheet_id: str = Field(min_length=1)
