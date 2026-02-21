from pydantic import BaseModel, ConfigDict, Field


class NoSQLConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    uri: str = Field(min_length=1)
    database: str = Field(min_length=1)
    collection: str | None = None
    username: str | None = None
    password: str | None = None


class CassandraConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    hosts: list[str] = Field(min_length=1)
    port: int = Field(default=9042, ge=1, le=65535)
    keyspace: str = Field(min_length=1)
    username: str | None = None
    password: str | None = None


class Neo4jConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    uri: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    database: str = Field(default="neo4j", min_length=1)
