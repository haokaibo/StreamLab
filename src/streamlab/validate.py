"""Business-rule validation applied before a record is allowed on the raw topic.

Records that fail go to the dead-letter topic with the list of problems, instead of
being silently dropped or poisoning downstream jobs.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta
from typing import Any


def validate_sample(
    record: dict[str, Any],
    *,
    now: datetime | None = None,
    max_future_s: float = 300.0,
) -> list[str]:
    """Return a list of human-readable problems; empty list means the record is valid."""
    problems: list[str] = []
    for field in ("event_id", "host", "source", "metric"):
        value = record.get(field)
        if not isinstance(value, str) or not value.strip():
            problems.append(f"{field} must be a non-empty string")

    value = record.get("value")
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        problems.append("value must be a number")
    elif not math.isfinite(float(value)):
        problems.append("value must be finite")

    event_time = record.get("event_time")
    if not isinstance(event_time, datetime) or event_time.tzinfo is None:
        problems.append("event_time must be a timezone-aware datetime")
    elif now is not None and event_time > now + timedelta(seconds=max_future_s):
        problems.append(f"event_time is more than {max_future_s:.0f}s in the future")

    return problems
