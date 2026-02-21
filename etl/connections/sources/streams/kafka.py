import asyncio
import time
from typing import Any

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestionResult
from ._batch import build_record, build_success_result
from .config import KafkaConfig


class KafkaConnector(BaseConnector):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        security_protocol: str = "PLAINTEXT",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        lake_path: str = "./lake",
        max_messages: int = 500,
        max_wait_seconds: float = 5.0,
        poll_timeout_seconds: float = 1.0,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "KAFKA",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("bootstrap_servers", "topic", "group_id"),
            defaults={
                "security_protocol": security_protocol,
                "auto_offset_reset": auto_offset_reset,
                "enable_auto_commit": enable_auto_commit,
                "lake_path": lake_path,
                "max_messages": max_messages,
                "max_wait_seconds": max_wait_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            },
            overrides={
                "bootstrap_servers": bootstrap_servers,
                "topic": topic,
                "group_id": group_id,
                "security_protocol": security_protocol,
                "auto_offset_reset": auto_offset_reset,
                "enable_auto_commit": enable_auto_commit,
                "lake_path": lake_path,
                "max_messages": max_messages,
                "max_wait_seconds": max_wait_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            },
        )
        self.config = KafkaConfig.model_validate(merged_config)
        self.logger = get_logger("sources.streams.kafka")
        self._consumer: Any | None = None

    def connect(self) -> None:
        try:
            from confluent_kafka import Consumer  # type: ignore
        except ImportError as exc:
            raise RuntimeError("confluent-kafka is not installed. Add it to requirements to use KafkaConnector.") from exc

        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting Kafka connector with config=%s", safe_config)

        consumer_config = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "group.id": self.config.group_id,
            "security.protocol": self.config.security_protocol,
            "auto.offset.reset": self.config.auto_offset_reset,
            "enable.auto.commit": self.config.enable_auto_commit,
        }
        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe([self.config.topic])
        self.logger.info("Kafka connector subscribed to topic=%s", self.config.topic)

    def fetch_data(self, query: str) -> IngestionResult:
        if self._consumer is None:
            raise RuntimeError("Kafka connector is not connected. Call connect() first.")

        stream_name = query.strip() or self.config.topic
        if stream_name != self.config.topic:
            self._consumer.subscribe([stream_name])

        records = self._consume_micro_batch(stream_name)
        return build_success_result(
            protocol="kafka",
            stream_name=stream_name,
            records=records,
            lake_path=self.config.lake_path,
        )

    async def run_async(self, stop_event: asyncio.Event | None = None, max_iterations: int | None = None) -> list[IngestionResult]:
        results: list[IngestionResult] = []
        iteration = 0

        while True:
            if stop_event is not None and stop_event.is_set():
                break
            if max_iterations is not None and iteration >= max_iterations:
                break

            result = await asyncio.to_thread(self.fetch_data, "")
            if result.items:
                results.append(result)

            iteration += 1

        return results

    def close(self) -> None:
        self.logger.info("Closing Kafka connector")
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def _consume_micro_batch(self, stream_name: str) -> list[dict[str, str | int | float | bool | None]]:
        if self._consumer is None:
            raise RuntimeError("Kafka connector is not connected. Call connect() first.")

        start = time.monotonic()
        records: list[dict[str, str | int | float | bool | None]] = []

        while len(records) < self.config.max_messages and (time.monotonic() - start) < self.config.max_wait_seconds:
            message = self._consumer.poll(self.config.poll_timeout_seconds)
            if message is None:
                continue

            if message.error() is not None:
                self.logger.error("Kafka consume error=%s", message.error())
                raise RuntimeError(f"Kafka consume error: {message.error()}")

            records.append(
                build_record(
                    protocol="kafka",
                    stream_name=stream_name,
                    payload=message.value(),
                    message_key=message.key(),
                    metadata={
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                )
            )

        self.logger.info("Kafka micro-batch consumed topic=%s messages=%s", stream_name, len(records))
        return records
