"""Kafka producer with Avro + Schema Registry and a dead-letter path."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from streamlab.schemas import TELEMETRY_V1, load_schema_text
from streamlab.validate import validate_sample

log = logging.getLogger(__name__)

RAW_TOPIC = "telemetry.raw.v1"
DLQ_TOPIC = "telemetry.dlq.v1"


def build_serializer(registry: Any, schema_name: str = TELEMETRY_V1) -> Any:
    """Avro serializer bound to ``registry`` (a SchemaRegistryClient or a mock of it).

    Subject naming is the default TopicNameStrategy: ``<topic>-value``.
    """
    from confluent_kafka.schema_registry.avro import AvroSerializer

    return AvroSerializer(
        registry,
        load_schema_text(schema_name),
        conf={"auto.register.schemas": True},
    )


class TelemetryProducer:
    """Sends valid records to the raw topic and invalid ones to the DLQ topic."""

    def __init__(
        self,
        bootstrap: str,
        registry_url: str,
        *,
        raw_topic: str = RAW_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
        schema_name: str = TELEMETRY_V1,
    ) -> None:
        # Imported lazily so unit tests and `check-schemas` work without librdkafka.
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient

        self._raw_topic = raw_topic
        self._dlq_topic = dlq_topic
        registry = SchemaRegistryClient({"url": registry_url})
        self._serializer = build_serializer(registry, schema_name)
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": "streamlab-producer",
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
            }
        )
        self.sent = 0
        self.dead_lettered = 0
        self.delivery_errors = 0

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self.delivery_errors += 1
            log.error("delivery failed: %s", err)

    def send(self, record: dict[str, Any], *, now: datetime | None = None) -> bool:
        """Validate and send. Returns True if sent to the raw topic, False if dead-lettered."""
        from confluent_kafka.serialization import MessageField, SerializationContext

        problems = validate_sample(record, now=now)
        key = f"{record.get('host', '')}|{record.get('metric', '')}".encode()
        if problems:
            payload = {"problems": problems, "record": record}
            self._producer.produce(
                self._dlq_topic,
                key=key,
                value=json.dumps(payload, default=str).encode(),
                on_delivery=self._on_delivery,
            )
            self.dead_lettered += 1
            self._producer.poll(0)
            return False

        value = self._serializer(record, SerializationContext(self._raw_topic, MessageField.VALUE))
        self._producer.produce(self._raw_topic, key=key, value=value, on_delivery=self._on_delivery)
        self.sent += 1
        self._producer.poll(0)
        return True

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)
