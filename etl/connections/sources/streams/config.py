from pydantic import BaseModel, ConfigDict, Field


class StreamsConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bootstrap_servers: str = Field(min_length=1)
    topic: str = Field(min_length=1)
    group_id: str | None = None
    security_protocol: str = Field(default="PLAINTEXT", min_length=1)
    lake_path: str = Field(default="./lake", min_length=1)
    max_messages: int = Field(default=500, ge=1)
    max_wait_seconds: float = Field(default=5.0, gt=0)
    poll_timeout_seconds: float = Field(default=1.0, gt=0)


class KafkaConfig(StreamsConfig):
    auto_offset_reset: str = Field(default="earliest", min_length=1)
    enable_auto_commit: bool = False


class AMQPConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    host: str = Field(min_length=1)
    port: int = Field(default=5672, ge=1, le=65535)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    virtual_host: str = Field(default="/", min_length=1)
    queue: str = Field(min_length=1)
    lake_path: str = Field(default="./lake", min_length=1)
    max_messages: int = Field(default=500, ge=1)
    max_wait_seconds: float = Field(default=5.0, gt=0)
    poll_timeout_seconds: float = Field(default=1.0, gt=0)


class NATSConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    servers: list[str] = Field(min_length=1)
    subject: str = Field(min_length=1)
    queue_group: str | None = None
    lake_path: str = Field(default="./lake", min_length=1)
    max_messages: int = Field(default=500, ge=1)
    max_wait_seconds: float = Field(default=5.0, gt=0)
    poll_timeout_seconds: float = Field(default=1.0, gt=0)
