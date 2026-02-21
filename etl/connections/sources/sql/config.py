from pydantic import BaseModel, ConfigDict, Field


class SQLiteConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    database_path: str = Field(min_length=1)


class PostgresConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=5432, ge=1, le=65535)
    database: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


class MSSQLConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=1433, ge=1, le=65535)
    database: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    driver: str = Field(default="ODBC Driver 18 for SQL Server", min_length=1)
    trust_server_certificate: str = Field(default="yes", min_length=1)


class OracleConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=1521, ge=1, le=65535)
    service_name: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
