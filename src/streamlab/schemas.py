"""Load and validate the Avro schemas that live in ``schemas/avro``."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import fastavro

TELEMETRY_V1 = "telemetry_sample.v1.avsc"
TELEMETRY_V2_CANDIDATE = "evolution/telemetry_sample.v2-candidate.avsc"


def schema_dir() -> Path:
    """Directory holding .avsc files. Override with STREAMLAB_SCHEMA_DIR."""
    override = os.environ.get("STREAMLAB_SCHEMA_DIR")
    if override:
        return Path(override)
    # src/streamlab/schemas.py -> repo root is three parents up (editable install).
    return Path(__file__).resolve().parents[2] / "schemas" / "avro"


def load_schema_text(name: str = TELEMETRY_V1) -> str:
    return (schema_dir() / name).read_text(encoding="utf-8")


def load_schema(name: str = TELEMETRY_V1) -> dict[str, Any]:
    """Return the parsed (canonicalised) schema; raises if the .avsc is invalid."""
    return fastavro.parse_schema(json.loads(load_schema_text(name)))


def validate_record(record: dict[str, Any], name: str = TELEMETRY_V1) -> None:
    """Raise fastavro's ValidationError if ``record`` does not match the schema."""
    fastavro.validate(record, load_schema(name), raise_errors=True)
