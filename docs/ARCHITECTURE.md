# Architecture (Phase 1)

```
                 ┌────────────┐  Avro + Schema Registry   ┌──────────────────────┐
 generator ────► │  producer  │ ───────────────────────► │ telemetry.raw.v1 (3p) │
 replay (JSONL)  │ validate + │ ── invalid ─► telemetry.dlq.v1 (JSON)             │
                 └────────────┘                           └──────────┬───────────┘
                                                                     │
                                    Flink job A (10_job_clean.sql)   ▼
                                    dedup by event_id  ──────────► telemetry.clean.v1 (3p)
                                    raw copy ───────────────────► ./data/raw-archive (JSON on a bind mount)
                                                                     │
                                    Flink job B (20_job_agg_1m.sql)  ▼
                                    1-min event-time tumbling windows
                                                                     │ JDBC upsert
                                                                     ▼
                                               PostgreSQL agg_1m ──► Grafana
```

## Topics

| Topic | Partitions | Key | Value | Notes |
|---|---|---|---|---|
| `telemetry.raw.v1` | 3 | `host\|metric` | Avro (`telemetry_sample.v1`) | at-least-once, idempotent producer, may contain duplicates / lateness |
| `telemetry.clean.v1` | 3 | none | Avro (Flink-derived schema) | deduplicated, written with exactly-once transactions |
| `telemetry.dlq.v1` | 1 | `host\|metric` | JSON `{problems, record}` | rejected by producer-side validation |

## Guarantees and policies

* **Delivery**: producer is idempotent (`acks=all`); Flink job A has checkpoints every 10 s.
* **Duplicates**: dropped by event id, first arrival wins, state TTL 2 h (duplicates arriving
  later than that are not detected: a documented trade-off).
* **Out-of-order**: tolerated up to the 30 s watermark delay.
* **Late**: events later than the watermark are dropped by the window operator in Phase 1
  (counted in the ground truth as not expected). Side-output capture: roadmap T-05.
* **Sink idempotency**: `agg_1m` primary key `(window_start, host, metric)`; the JDBC sink
  upserts, so a restarted job overwrites rather than duplicates a window.
* **Replay**: every raw event (duplicates included) is archived as JSON by hour, so jobs can
  be rebuilt from files.

## Correctness oracle

`src/streamlab/generator.py` produces both the messy stream and the truth. Rules that must
stay aligned with the SQL: watermark delay 30 s, window 60 s, out-of-order delay < 30 s,
late delay >= 120 s, bad records go to the DLQ. `streamlab verify` compares closed windows
in `agg_1m` with the expected counts and averages.

## Memory budget (Docker VM)

| Service | mem_limit |
|---|---|
| kafka | 768 MB |
| kafka-init (one-shot) | 512 MB |
| schema-registry | 512 MB |
| postgres | 384 MB |
| flink-jobmanager | 1.5 GB |
| flink-taskmanager | 2 GB |
| flink-job-* (one-shot x2) | 768 MB each |
| grafana | 384 MB |
| kafka-ui (profile `ui`) | 512 MB |

Steady state is about 5.5 GB; limits of every service sum to below 10 GB (checked by
`tests/test_compose.py`). A local Kubernetes cluster in Phase 2 needs its own budget.

## Ports

Kafka 9092 · Schema Registry 8081 · Flink UI 8082 · PostgreSQL **55432** ·
Grafana 3000 · Kafka UI 8080 (profile `ui`).

PostgreSQL is published on 55432 rather than 5432 because a locally installed Postgres
commonly owns 5432, and the clash silently points `streamlab verify` at the wrong server.
Inside the container the port is still 5432.
