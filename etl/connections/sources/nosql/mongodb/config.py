from pydantic import BaseModel, ConfigDict, Field


class MongoDBConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=27017, ge=1, le=65535)
    username: str | None = None
    password: str | None = None
    database: str = Field(min_length=1)
    auth_source: str | None = None
