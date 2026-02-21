from pydantic import BaseModel, ConfigDict, Field


class IngestedItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    source_path: str | None = None
    lake_path: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    payload: dict | list | str | int | float | bool | None = None


class IngestionResult(BaseModel):
    model_config = ConfigDict(extra="ignore")

    protocol: str = Field(min_length=1)
    success: bool
    items: list[IngestedItem] = Field(default_factory=list)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    error_message: str | None = None
