# streamlab

A single-machine practice platform for production streaming concerns: **Kafka (Avro +
Schema Registry) → Flink SQL → PostgreSQL / local file archive → Grafana**, with a
deterministic data generator that doubles as a correctness oracle.

> Status: Phase 1, running and verified. The whole stack has been brought up on Apple
> Silicon, both Flink jobs run, and the end-to-end check passes against ground truth
> (see [Verification evidence](#verification-evidence)). Remaining Phase 1 work: T-02,
> real sensor replay (`docs/ROADMAP.md`).

## What it demonstrates

* Event-time processing with watermarks; out-of-order and late events handled by policy.
* Deduplication by event id (at-least-once producers, effectively-once results).
* Exactly-once Kafka sink (transactions + checkpoints) and idempotent PostgreSQL upserts.
* Checkpoint recovery: killing the only TaskManager mid-run loses and duplicates nothing.
* Avro schemas in a registry with BACKWARD compatibility checks.
* Replayable raw JSON layer on the local filesystem; dead-letter topic for bad records.
* Ground-truth verification: `streamlab verify` proves the database matches what the
  generator knows it sent, duplicates and lateness included.

## Quick start (Apple Silicon or Linux, Docker with ~10 GB available)

```bash
make setup                 # venv, dependencies, .env
source .venv/bin/activate
make up                    # build the Flink image, start everything
make smoke                 # generate messy data, wait, verify against ground truth
```

`make smoke` prints `OK: database aggregates match ground truth for all closed windows`
when the pipeline agrees with the generator. It takes about 5 minutes: it runs the
generator in `--mode realtime` for 120 s plus a 90 s clean tail, then waits for windows to
close. Override with environment variables:

```bash
DURATION=300 SETTLE=90 WAIT=120 make smoke       # longer run
make smoke -- --dup 0.2 --ooo 0.3                # heavier duplicate / out-of-order load
```

When passing extra arguments, `make` also prints `No rule to make target '--dup'` after the
run finishes. The smoke script itself completes and prints its verdict first; the message is
make parsing the arguments that follow the target and is harmless.

UIs and ports:

| Service | URL |
|---|---|
| Flink web UI | http://localhost:8082 |
| Grafana (admin/admin) | http://localhost:3000 |
| Schema Registry | http://localhost:8081 |
| PostgreSQL | `localhost:55432` (not 5432, see note below) |
| Kafka (from the host) | `localhost:9092` |
| Kafka UI (`make ui`) | http://localhost:8080 |

PostgreSQL is published on **55432** because a locally installed Postgres commonly owns
5432, and the clash silently points `streamlab verify` at the wrong server. Inside the
Docker network the port is still 5432.

Without Docker: `make test` runs the unit tests (generator, schemas, validation, replay,
verification logic, and static consistency checks of the infrastructure files).

## Verification evidence

Actually run on 2026-11-18 on Apple Silicon, not inferred from unit tests:

| Check | Result |
|---|---|
| `docker compose ps` | kafka, schema-registry, postgres, flink-jobmanager healthy; one-shot jobs exit 0 |
| Flink jobs | `streamlab-clean` and `streamlab-agg-1m` both RUNNING |
| `make smoke` (defaults) | sent 1703, dead-lettered 3 → `verify` OK |
| `make smoke -- --dup 0.2 --ooo 0.3` | sent 1698, dead-lettered 5 → `verify` OK |
| Grafana panels | all four return rows through the datasource query API |
| TaskManager killed mid-run | jobs went RESTARTING, came back RUNNING, `verify` OK; checkpoints 95 completed / 4 failed / 6 restored |

Each fix that was needed to get here is recorded with its symptom, cause and resolution in
`docs/KNOWN-UNKNOWNS.md`.

## How it works

```
generator / replay ──► Kafka (Avro + Schema Registry) ──► Flink SQL ──► PostgreSQL ──► Grafana
                        raw ─► [A: dedup] ─► clean ─► [B: 1-min event-time windows]
                                     └──► JSON archive under ./data/raw-archive
```

* **Job A** (`flink/sql/10_job_clean.sql`) deduplicates `telemetry.raw.v1` by `event_id`
  into `telemetry.clean.v1` (exactly-once Kafka sink) and archives every raw record as JSON.
* **Job B** (`flink/sql/20_job_agg_1m.sql`) runs 1-minute event-time tumbling windows over
  `telemetry.clean.v1` and upserts them into `agg_1m` in PostgreSQL.
* **Storage** is the local filesystem: Flink's checkpoints and savepoints and the raw JSON
  archive live under `./data` (git-ignored). No object store or cloud account is required;
  see ADR-009 in `docs/DECISIONS.md`.

## Stack versions

Kafka 4.4.0 · Schema Registry 7.9.11 · Flink 2.2.1 · PostgreSQL 17.12 · Grafana 13.3.1,
with Flink connectors `flink-sql-connector-kafka` 5.0.0-2.2 and `flink-connector-jdbc`
4.1.0-2.2. Flink is pinned to 2.2.1, not the newest 2.3.0, because the JDBC connector has no
2.3 build yet; see ADR-008. Every image is pinned in `.env.example`, and
`tests/test_compose.py` guards the pins and the contract between the generator, the SQL and
the Avro schemas.

## Documentation

* `docs/ARCHITECTURE.md`: data flow, topics, guarantees, memory budget.
* `docs/DECISIONS.md`: why things are the way they are (ADRs).
* `docs/ROADMAP.md`: phases and agent-sized tasks with acceptance criteria.
* `docs/KNOWN-UNKNOWNS.md`: what was unverified, what broke on first start, and how it was fixed.
* `docs/TALKING-POINTS.md`: how to explain the design in an interview.
* `AGENTS.md`: rules for AI coding agents.

## Data and privacy

Phase 1 uses synthetic data and sanitised replays only. The repository must never contain
real store, camera, customer or personal data.
