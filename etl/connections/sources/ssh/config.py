from pydantic import BaseModel, ConfigDict, Field


class SSHConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=22, ge=1, le=65535)
    username: str = Field(min_length=1)
    password: str | None = None
    private_key_path: str | None = None
    remote_base_path: str = "."
    lake_path: str = Field(default="./lake", min_length=1)
