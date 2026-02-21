from pydantic import BaseModel, ConfigDict, Field


class FTPConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=21, ge=1, le=65535)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    passive_mode: bool = True
    remote_base_path: str = "/"
    lake_path: str = Field(default="./lake", min_length=1)


class WebDAVConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    base_url: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
