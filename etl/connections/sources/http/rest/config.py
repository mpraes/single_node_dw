from typing import Literal

from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field


class HTTPConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    base_url: AnyHttpUrl
    token: str | None = None
    timeout_seconds: int = Field(default=30, ge=1)
    client_library: Literal["requests", "httpx"] = "requests"
