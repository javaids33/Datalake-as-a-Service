"""
Kafka bridge — consume topics, micro-batch write to Iceberg.
Optional: enabled when DLS_KAFKA_ENABLED=true.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import pyarrow as pa

from dlsidecar.config import settings
from dlsidecar.governance.audit import emit_audit_event

logger = logging.getLogger("dlsidecar.streaming.kafka_bridge")


class KafkaBridge:
    """Consumes Kafka topics and micro-batch writes to Iceberg."""

    def __init__(self, iceberg_manager=None):
        self._iceberg = iceberg_manager
        self._consumer = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._batch: list[dict] = []
        self._last_flush = time.monotonic()

    @property
    def enabled(self) -> bool:
        return settings.kafka_enabled and bool(settings.kafka_brokers)

    async def start(self) -> None:
        if not self.enabled:
            logger.info("Kafka bridge not enabled, skipping")
            return

        try:
            from confluent_kafka import Consumer

            import os

            conf = {
                "bootstrap.servers": settings.kafka_brokers,
                "group.id": settings.kafka_consumer_group or f"dlsidecar-{os.environ.get('POD_NAME', 'local')}",
                "auto.offset.reset": "earliest",
            }
            self._consumer = Consumer(conf)
            topics = [t.strip() for t in settings.kafka_consumer_topics.split(",") if t.strip()]
            self._consumer.subscribe(topics)
            self._running = True
            self._task = asyncio.create_task(self._consume_loop())
            logger.info("Kafka bridge started: topics=%s", topics)
        except ImportError:
            logger.warning("confluent-kafka not installed, Kafka bridge disabled")
        except Exception as exc:
            logger.error("Failed to start Kafka bridge: %s", exc)

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            self._consumer.close()

    async def _consume_loop(self) -> None:
        while self._running:
            try:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    # Check flush interval
                    if self._batch and (time.monotonic() - self._last_flush) >= settings.kafka_flush_interval_seconds:
                        await self._flush()
                    continue

                if msg.error():
                    logger.error("Kafka error: %s", msg.error())
                    continue

                record = self._deserialize(msg.value())
                if record:
                    self._batch.append(record)

                # Flush on batch size
                if len(self._batch) >= settings.kafka_batch_size:
                    await self._flush()

                # Flush on interval
                if (time.monotonic() - self._last_flush) >= settings.kafka_flush_interval_seconds:
                    await self._flush()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Kafka consume error: %s", exc)
                await asyncio.sleep(1)

        # Final flush
        if self._batch:
            await self._flush()

    def _deserialize(self, value: bytes) -> dict | None:
        """Deserialize message based on configured format."""
        if settings.kafka_format == "json":
            try:
                return json.loads(value)
            except Exception:
                return None
        elif settings.kafka_format == "avro":
            try:
                import fastavro
                import io

                reader = fastavro.reader(io.BytesIO(value))
                for record in reader:
                    return record
            except Exception:
                return None
        return None

    async def _flush(self) -> None:
        """Flush accumulated batch to Iceberg."""
        if not self._batch:
            return

        batch = self._batch
        self._batch = []
        self._last_flush = time.monotonic()

        try:
            table = pa.Table.from_pylist(batch)

            target = settings.kafka_iceberg_target
            if not target:
                logger.warning("DLS_KAFKA_ICEBERG_TARGET not set, dropping %d records", len(batch))
                return

            parts = target.split(".")
            database = parts[0] if len(parts) > 1 else "default"
            table_name = parts[-1]

            if self._iceberg:
                self._iceberg.write(
                    database=database,
                    table_name=table_name,
                    data=table,
                    mode="append",
                )

            emit_audit_event(
                "stream",
                engine="kafka",
                tables_written=[target],
                extra={"rows_written": len(batch), "source": "kafka"},
            )
            logger.info("Kafka flush: %d records → %s", len(batch), target)
        except Exception as exc:
            logger.error("Kafka flush failed: %s", exc)
