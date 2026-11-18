"""Serialization path of the producer, against confluent-kafka's in-memory mock registry."""

from datetime import UTC, datetime

import pytest

pytest.importorskip("confluent_kafka.schema_registry")

from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AvroDeserializer  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from streamlab.kafka_io import RAW_TOPIC, build_serializer  # noqa: E402

RECORD = {
    "event_id": "e-1",
    "event_time": datetime(2026, 1, 1, 12, 0, 0, 123000, tzinfo=UTC),
    "host": "red-one",
    "source": "synthetic",
    "metric": "gpu.junction_temp_c",
    "value": 71.5,
    "unit": "C",
    "seq": 7,
    "labels": {"device": "0"},
}


def test_confluent_wire_format_roundtrip_and_subject():
    registry = SchemaRegistryClient.new_client({"url": "mock://streamlab-test"})
    serializer = build_serializer(registry)
    ctx = SerializationContext(RAW_TOPIC, MessageField.VALUE)

    payload = serializer(RECORD, ctx)

    assert payload[0] == 0  # Confluent magic byte
    assert f"{RAW_TOPIC}-value" in registry.get_subjects()
    decoded = AvroDeserializer(registry)(payload, ctx)
    assert decoded["event_id"] == "e-1"
    assert decoded["event_time"] == RECORD["event_time"]  # millisecond timestamp preserved
    assert decoded["labels"] == {"device": "0"}
