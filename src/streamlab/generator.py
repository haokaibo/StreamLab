"""Deterministic synthetic telemetry with controlled mess.

The generator is the test oracle for the whole pipeline. For a given seed it produces:

* ``emissions``: what a producer should send, in arrival order, including
  duplicates, out-of-order, very late and bad records;
* ``truth``: one entry per unique event, with ``counted`` saying whether the
  pipeline is expected to include it in the 1-minute aggregates.

Rules that make ``counted`` predictable (they must match flink/sql/*.sql):

* Flink watermark delay is ``WATERMARK_DELAY_S`` (30 s).
* ``out_of_order`` events arrive at most ``max_out_of_order_s`` (< 30 s) late: counted.
* ``late`` events arrive at least ``late_min_s`` (>= 120 s) late, which is beyond the
  watermark delay plus the 60 s window: dropped by the windowed job, so not counted.
* ``bad`` events are rejected by the producer (dead-letter topic): not counted.
* ``duplicate`` re-sends of an event carry the same ``event_id``: counted once.
"""

from __future__ import annotations

import math
import random
import uuid
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

WATERMARK_DELAY_S = 30
WINDOW_S = 60


@dataclass(frozen=True)
class MetricSpec:
    name: str
    unit: str
    base: float
    amplitude: float
    period_s: float
    noise: float


DEFAULT_METRICS: tuple[MetricSpec, ...] = (
    MetricSpec("gpu.junction_temp_c", "C", 72.0, 12.0, 600.0, 0.8),
    MetricSpec("gpu.vram_temp_c", "C", 70.0, 10.0, 600.0, 0.8),
    MetricSpec("gpu.power_w", "W", 180.0, 60.0, 300.0, 4.0),
    MetricSpec("cpu.package_temp_c", "C", 55.0, 15.0, 420.0, 1.0),
)


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    start: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    duration_s: int = 300  # disturbed section
    settle_s: int = 90  # clean tail so the last windows can close
    interval_s: float = 1.0  # one sample per series per interval
    hosts: tuple[str, ...] = ("red-one", "g3")
    metrics: tuple[MetricSpec, ...] = DEFAULT_METRICS
    source: str = "synthetic"
    duplicate_ratio: float = 0.02
    out_of_order_ratio: float = 0.10
    max_out_of_order_s: float = 20.0
    late_ratio: float = 0.01
    late_min_s: float = 120.0
    late_max_s: float = 240.0
    bad_ratio: float = 0.005


@dataclass(frozen=True)
class Emission:
    emit_offset_s: float  # seconds after run start at which to produce this record
    record: dict[str, Any]
    kind: str  # normal | out_of_order | late | duplicate | bad


@dataclass
class GeneratorRun:
    config: GeneratorConfig
    emissions: list[Emission] = field(default_factory=list)
    truth: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total_duration_s(self) -> int:
        return self.config.duration_s + self.config.settle_s


def _stable_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generate(config: GeneratorConfig) -> GeneratorRun:
    rng = random.Random(config.seed)
    run = GeneratorRun(config=config)
    series = [(h, m) for h in config.hosts for m in config.metrics]
    seq: dict[tuple[str, str], int] = defaultdict(int)
    n_ticks = int((config.duration_s + config.settle_s) / config.interval_s)
    disturbed_until = config.duration_s
    # late events must arrive while the run is still producing newer data
    late_cutoff = disturbed_until - config.late_max_s

    for tick in range(n_ticks):
        offset = tick * config.interval_s
        disturbed = offset < disturbed_until
        for idx, (host, spec) in enumerate(series):
            event_time = config.start + timedelta(seconds=offset)
            phase = idx * 0.7
            value = (
                spec.base
                + spec.amplitude * math.sin(2 * math.pi * offset / spec.period_s + phase)
                + rng.gauss(0.0, spec.noise)
            )
            key = (host, spec.name)
            seq[key] += 1
            record: dict[str, Any] = {
                "event_id": _stable_uuid(rng),
                "event_time": event_time,
                "host": host,
                "source": config.source,
                "metric": spec.name,
                "value": round(value, 3),
                "unit": spec.unit,
                "seq": seq[key],
                "labels": {},
            }

            kind = "normal"
            delay = rng.uniform(0.0, 0.2)
            if disturbed:
                roll = rng.random()
                bad_edge = config.bad_ratio
                late_edge = bad_edge + config.late_ratio
                ooo_edge = late_edge + config.out_of_order_ratio
                if roll < bad_edge:
                    kind = "bad"
                    if rng.random() < 0.5:
                        record["value"] = float("nan")
                    else:
                        record["host"] = ""
                elif roll < late_edge and offset <= late_cutoff:
                    kind = "late"
                    delay = rng.uniform(config.late_min_s, config.late_max_s)
                elif bad_edge + config.late_ratio <= roll < ooo_edge:
                    kind = "out_of_order"
                    delay = rng.uniform(1.0, config.max_out_of_order_s)

            run.emissions.append(Emission(offset + delay, record, kind))
            run.truth.append(
                {
                    "event_id": record["event_id"],
                    "event_time": event_time,
                    "host": host,
                    "metric": spec.name,
                    "value": record["value"],
                    "counted": kind in ("normal", "out_of_order"),
                    "kind": kind,
                }
            )

            if disturbed and kind != "bad" and rng.random() < config.duplicate_ratio:
                dup_delay = delay + rng.uniform(0.05, 5.0)
                run.emissions.append(Emission(offset + dup_delay, dict(record), "duplicate"))

    run.emissions.sort(key=lambda e: e.emit_offset_s)
    return run


def expected_aggregates(
    truth: list[dict[str, Any]],
    *,
    window_s: int = WINDOW_S,
    watermark_delay_s: int = WATERMARK_DELAY_S,
    margin_s: int = 10,
) -> dict[tuple[datetime, str, str], dict[str, float]]:
    """Expected 1-minute aggregates for *closed* windows, keyed by
    (window_start as naive UTC, host, metric).

    A window is considered closed once ``window_end <= max_event_time - watermark_delay -
    margin``; windows that may still be open when the run ends are left out so that the
    comparison with the database is not flaky.
    """
    if not truth:
        return {}
    max_event_time = max(t["event_time"] for t in truth)
    closed_before = max_event_time - timedelta(seconds=watermark_delay_s + margin_s)

    buckets: dict[tuple[datetime, str, str], list[float]] = defaultdict(list)
    for t in truth:
        if not t["counted"]:
            continue
        ts = t["event_time"].astimezone(UTC)
        epoch = int(ts.timestamp())
        start_epoch = epoch - (epoch % window_s)
        window_start = datetime.fromtimestamp(start_epoch, tz=UTC)
        if window_start + timedelta(seconds=window_s) > closed_before:
            continue
        buckets[(window_start.replace(tzinfo=None), t["host"], t["metric"])].append(t["value"])

    return {
        k: {
            "count": float(len(v)),
            "avg": sum(v) / len(v),
            "min": min(v),
            "max": max(v),
        }
        for k, v in buckets.items()
    }


def iter_records(run: GeneratorRun) -> Iterator[Emission]:
    yield from run.emissions
