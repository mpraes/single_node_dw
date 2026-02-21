import asyncio
from typing import Any

from ..._config import load_connection_config
from ..._logging import get_logger, redact_config
from ..base_connector import BaseConnector
from ..data_contract import IngestionResult
from ._batch import build_record, build_success_result
from .config import NATSConfig


class NATSConnector(BaseConnector):
    def __init__(
        self,
        servers: list[str],
        subject: str,
        queue_group: str | None = None,
        lake_path: str = "./lake",
        max_messages: int = 500,
        max_wait_seconds: float = 5.0,
        poll_timeout_seconds: float = 1.0,
        config: dict | None = None,
        file_path: str | None = None,
        env_prefix: str = "NATS",
    ):
        merged_config = load_connection_config(
            config,
            file_path=file_path,
            env_prefix=env_prefix,
            required=("servers", "subject"),
            defaults={
                "queue_group": queue_group,
                "lake_path": lake_path,
                "max_messages": max_messages,
                "max_wait_seconds": max_wait_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            },
            overrides={
                "servers": servers,
                "subject": subject,
                "queue_group": queue_group,
                "lake_path": lake_path,
                "max_messages": max_messages,
                "max_wait_seconds": max_wait_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            },
        )
        self.config = NATSConfig.model_validate(merged_config)
        self.logger = get_logger("sources.streams.nats")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connection: Any | None = None

    def connect(self) -> None:
        safe_config = redact_config(self.config.model_dump())
        self.logger.info("Connecting NATS connector with config=%s", safe_config)

        self._loop = asyncio.new_event_loop()
        self._connection = self._run_coroutine(self._connect_async())
        self.logger.info("NATS connector connected subject=%s", self.config.subject)

    def fetch_data(self, query: str) -> IngestionResult:
        if self._connection is None:
            raise RuntimeError("NATS connector is not connected. Call connect() first.")

        stream_name = query.strip() or self.config.subject
        records = self._run_coroutine(self._consume_micro_batch(stream_name))

        return build_success_result(
            protocol="nats",
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
        self.logger.info("Closing NATS connector")
        if self._connection is not None:
            self._run_coroutine(self._close_async())
            self._connection = None

        if self._loop is not None:
            self._loop.close()
            self._loop = None

    def _run_coroutine(self, coroutine: Any) -> Any:
        if self._loop is None:
            raise RuntimeError("NATS event loop not initialized. Call connect() first.")
        return self._loop.run_until_complete(coroutine)

    async def _connect_async(self) -> Any:
        try:
            from nats import connect as nats_connect  # type: ignore
        except ImportError as exc:
            raise RuntimeError("nats-py is not installed. Add it to requirements to use NATSConnector.") from exc

        return await nats_connect(servers=self.config.servers)

    async def _close_async(self) -> None:
        if self._connection is not None:
            await self._connection.drain()
            await self._connection.close()

    async def _consume_micro_batch(self, stream_name: str) -> list[dict[str, str | int | float | bool | None]]:
        if self._connection is None:
            raise RuntimeError("NATS connector is not connected. Call connect() first.")

        records: list[dict[str, str | int | float | bool | None]] = []
        subscription = await self._connection.subscribe(stream_name, queue=self.config.queue_group)
        deadline = asyncio.get_running_loop().time() + self.config.max_wait_seconds

        while len(records) < self.config.max_messages and asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            timeout = min(self.config.poll_timeout_seconds, max(remaining, 0.01))

            try:
                message = await subscription.next_msg(timeout=timeout)
            except Exception:
                continue

            records.append(
                build_record(
                    protocol="nats",
                    stream_name=stream_name,
                    payload=message.data,
                    message_key=message.subject,
                    metadata={
                        "subject": message.subject,
                        "reply": message.reply,
                    },
                )
            )

        await subscription.unsubscribe()
        self.logger.info("NATS micro-batch consumed subject=%s messages=%s", stream_name, len(records))
        return records
