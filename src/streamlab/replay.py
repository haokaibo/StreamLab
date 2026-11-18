"""Turn recorded measurements into telemetry events.

Supported input: JSON Lines, one object per line, either

* wide:  {"ts": <epoch seconds or ISO-8601>, "host": "red-one",
          "readings": {"gpu_junction_c": 84.0, "gpu_power_w": 240.0}}
* long:  {"ts": ..., "host": "red-one", "metric": "gpu.junction_temp_c",
          "value": 84.0, "unit": "C"}

The real hw_monitor.py output format is not known to this repo yet; adapting it is
task T-02 in docs/ROADMAP.md. ``event_id`` is a UUIDv5 of (source, host, metric, ts),
so replaying the same file twice produces identical ids and is removed by the
deduplication step: replay is idempotent by construction.
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

NAMESPACE = uuid.UUID("6f1b2c3e-8a4d-4c5e-9f10-2d7a5b9c1e42")

_UNIT_BY_SUFFIX = {"_c": "C", "_w": "W", "_mhz": "MHz", "_pct": "%", "_rpm": "rpm"}


def _parse_ts(raw: Any) -> datetime:
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw), tz=UTC)
    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _unit_for(name: str) -> str:
    lowered = name.lower()
    for suffix, unit in _UNIT_BY_SUFFIX.items():
        if lowered.endswith(suffix):
            return unit
    return ""


def _metric_name(raw: str) -> str:
    return raw.replace("_", ".", 1) if "." not in raw else raw


def read_jsonl(path: str | Path, *, source: str = "hw_monitor") -> Iterator[dict[str, Any]]:
    seq: dict[tuple[str, str], int] = defaultdict(int)
    with open(path, encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ts = _parse_ts(obj["ts"])
            host = obj["host"]
            if "readings" in obj:
                items = [(k, v, _unit_for(k)) for k, v in obj["readings"].items()]
            elif "metric" in obj:
                items = [(obj["metric"], obj["value"], obj.get("unit") or _unit_for(obj["metric"]))]
            else:
                raise ValueError(f"line {line_no}: expected 'readings' or 'metric'")
            for name, value, unit in items:
                metric = _metric_name(name)
                key = (host, metric)
                seq[key] += 1
                ts_ms = int(ts.timestamp() * 1000)
                yield {
                    "event_id": str(uuid.uuid5(NAMESPACE, f"{source}|{host}|{metric}|{ts_ms}")),
                    "event_time": ts,
                    "host": host,
                    "source": source,
                    "metric": metric,
                    "value": float(value),
                    "unit": unit,
                    "seq": seq[key],
                    "labels": {},
                }
