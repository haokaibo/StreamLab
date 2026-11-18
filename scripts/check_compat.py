#!/usr/bin/env python3
"""Ask Schema Registry whether a candidate schema is compatible with the latest registered one.

Usage: python scripts/check_compat.py [candidate.avsc] [subject]
Defaults: schemas/avro/evolution/telemetry_sample.v2-candidate.avsc, telemetry.raw.v1-value
Exit code 0 = compatible, 1 = incompatible, 2 = registry error.
"""

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    candidate = Path(
        sys.argv[1]
        if len(sys.argv) > 1
        else ROOT / "schemas/avro/evolution/telemetry_sample.v2-candidate.avsc"
    )
    subject = sys.argv[2] if len(sys.argv) > 2 else "telemetry.raw.v1-value"
    body = json.dumps({"schema": candidate.read_text()}).encode()
    req = urllib.request.Request(
        f"{REGISTRY}/compatibility/subjects/{subject}/versions/latest",
        data=body,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.load(resp)
    except urllib.error.URLError as exc:
        print(f"registry error: {exc}", file=sys.stderr)
        return 2
    print(f"{candidate.name} vs {subject}: compatible={result.get('is_compatible')}")
    return 0 if result.get("is_compatible") else 1


if __name__ == "__main__":
    sys.exit(main())
