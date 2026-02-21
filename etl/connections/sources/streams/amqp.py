import asyncio
import time
from typing import Any

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestionResult
from ._batch import build_record, build_success_result
from .config import AMQPConfig


class AMQPConnector(BaseConnector):
    def __init__(
        self,
        host: str,
        queue: str,
        username: str,
        password: str,
        port: int = 5672,
        virtual_host: str = "/",
        lake_path: str = "./lake",
        max_messages: int = 500,
        max_wait_seconds: float = 5.0,
        poll_timeout_seconds: float = 1.0,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "AMQP",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("host", "queue", "username", "password"),
            defaults={
                "port": port,
                "virtual_host": virtual_host,
                "lake_path": lake_path,
                "max_messages": max_messages,
                "max_wait_seconds": max_wait_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            },
            overrides={
                "host": host,
                "queue": queue,
                "username": username,
                "password": password,
                "port": port,
                "virtual_host": virtual_host,
                "lake_path": lake_path,
                "max_messages": max_messages,
                "max_wait_seconds": max_wait_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            },
        )
        self.config = AMQPConfig.model_validate(merged_config)
        self.logger = get_logger("sources.streams.amqp")
        self._connection: Any | None = None
        self._channel: Any | None = None

    def connect(self) -> None:
        try:
            import pika  # type: ignore
        except ImportError as exc:
            raise RuntimeError("pika is not installed. Add it to requirements to use AMQPConnector.") from exc

        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting AMQP connector with config=%s", safe_config)

        credentials = pika.PlainCredentials(self.config.username, self.config.password)
        parameters = pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            virtual_host=self.config.virtual_host,
            credentials=credentials,
        )
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self.config.queue, durable=True)
        self.logger.info("AMQP connector connected queue=%s", self.config.queue)

    def fetch_data(self, query: str) -> IngestionResult:
        if self._channel is None:
            raise RuntimeError("AMQP connector is not connected. Call connect() first.")

        stream_name = query.strip() or self.config.queue
        records, delivery_tags = self._consume_micro_batch(stream_name)
        result = build_success_result(
            protocol="amqp",
            stream_name=stream_name,
            records=records,
            lake_path=self.config.lake_path,
        )

        for delivery_tag in delivery_tags:
            self._channel.basic_ack(delivery_tag)

        return result

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
        self.logger.info("Closing AMQP connector")
        if self._channel is not None:
            self._channel.close()
            self._channel = None

        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _consume_micro_batch(self, stream_name: str) -> tuple[list[dict[str, str | int | float | bool | None]], list[int]]:
        if self._channel is None:
            raise RuntimeError("AMQP connector is not connected. Call connect() first.")

        start = time.monotonic()
        records: list[dict[str, str | int | float | bool | None]] = []
        delivery_tags: list[int] = []

        while len(records) < self.config.max_messages and (time.monotonic() - start) < self.config.max_wait_seconds:
            method_frame, header_frame, body = self._channel.basic_get(queue=stream_name, auto_ack=False)
            if method_frame is None:
                time.sleep(min(self.config.poll_timeout_seconds, 0.2))
                continue

            delivery_tags.append(method_frame.delivery_tag)
            records.append(
                build_record(
                    protocol="amqp",
                    stream_name=stream_name,
                    payload=body,
                    metadata={
                        "delivery_tag": method_frame.delivery_tag,
                        "exchange": method_frame.exchange,
                        "routing_key": method_frame.routing_key,
                        "content_type": getattr(header_frame, "content_type", None),
                    },
                )
            )

        self.logger.info("AMQP micro-batch consumed queue=%s messages=%s", stream_name, len(records))
        return records, delivery_tags
