"""Read/write the ground-truth file produced by ``streamlab generate``."""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any


def write_truth(path: str | Path, truth: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for t in truth:
            value = t["value"]
            finite = isinstance(value, (int, float)) and math.isfinite(value)
            row = {
                **t,
                "event_time": t["event_time"].isoformat(),
                "value": value if finite else None,
            }
            fh.write(json.dumps(row) + "\n")


def read_truth(path: str | Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            row["event_time"] = datetime.fromisoformat(row["event_time"])
            out.append(row)
    return out
