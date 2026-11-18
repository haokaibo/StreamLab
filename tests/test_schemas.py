import io
import json
from datetime import UTC, datetime

import fastavro
import pytest

from streamlab import schemas

RECORD = {
    "event_id": "5b9d0a58-3f35-4a63-9f0a-1c3a8f6d2f11",
    "event_time": datetime(2026, 1, 1, tzinfo=UTC),
    "host": "red-one",
    "source": "synthetic",
    "metric": "gpu.junction_temp_c",
    "value": 71.5,
    "unit": "C",
    "seq": 1,
    "labels": {},
}


def test_all_schemas_parse():
    schemas.load_schema(schemas.TELEMETRY_V1)
    schemas.load_schema(schemas.TELEMETRY_V2_CANDIDATE)


def test_valid_record_passes():
    schemas.validate_record(RECORD)


def test_missing_required_field_fails():
    bad = {k: v for k, v in RECORD.items() if k != "host"}
    with pytest.raises(fastavro.validation.ValidationError):
        schemas.validate_record(bad)


def test_record_name_matches_flink_default():
    parsed = json.loads(schemas.load_schema_text())
    assert parsed["name"] == "record"
    assert parsed["namespace"] == "org.apache.flink.avro.generated"


def test_v2_candidate_is_backward_compatible_with_v1_data():
    """Data written with v1 must be readable with the v2 reader schema (new field gets default)."""
    writer = schemas.load_schema(schemas.TELEMETRY_V1)
    reader = schemas.load_schema(schemas.TELEMETRY_V2_CANDIDATE)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, writer, RECORD)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, writer, reader)
    assert decoded["quality"] == "OK"
    assert decoded["host"] == "red-one"
