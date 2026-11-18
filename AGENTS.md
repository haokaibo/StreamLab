# AGENTS.md

Instructions for AI coding agents (and humans) working in this repository. Read this file
first, then `docs/KNOWN-UNKNOWNS.md` if something does not work. Keep changes small.

## What this project is

`streamlab` is a single-machine practice platform for production streaming concerns:

```
generator / replay ──► Kafka (Avro + Schema Registry) ──► Flink SQL ──► PostgreSQL ──► Grafana
                         raw ─► [A: dedup] ─► clean ─► [B: 1-min event-time windows]
                                      └──► JSON archive on S3-compatible storage (MinIO)
```

Goal: hands-on, demonstrable experience with exactly-once delivery, deduplication,
late / out-of-order data, schema evolution, replay, data quality and CI. It is a learning
and portfolio project, **not** a production system. Phase 1 data source is synthetic
telemetry plus replayed host-sensor readings.

Non-goals (do not add without discussion): authentication/TLS, multi-broker clusters,
cloud deployment, a web UI, real customer/store data.

## Ground rules

1. **Never commit real data or secrets.** Synthetic or replayed-and-sanitised data only. No
   hostnames, IPs, credentials, camera frames or personal/store data. `.env` is git-ignored;
   only `.env.example` (dev-only credentials) is committed.
2. **Do not claim something works unless you ran it.** State exactly what you ran and what
   it printed. If you cannot run Docker, say so and mark the change *unverified*.
3. **One concern per change.** Small diffs, conventional commit messages (`feat:`, `fix:`,
   `docs:`, `test:`, `chore:`).
4. **Tests first for logic.** Anything in `src/` gets a unit test. Anything that changes
   what the pipeline outputs must keep `streamlab verify` passing (see Verification).
5. **Keep the contract in sync.** The generator's ground-truth rules, the Flink SQL and the
   Avro schema describe the same system. If you change one, change the others and the tests
   in `tests/test_compose.py` that guard them.

## Layout

| Path | What |
|---|---|
| `docker-compose.yml` | The platform. Every service has a `mem_limit`; total budget is checked by a test. |
| `schemas/avro/` | Avro schemas. `evolution/` holds candidate versions that are not registered by default. |
| `flink/Dockerfile`, `flink/sql/` | Flink image with connectors; SQL jobs `10_job_clean.sql`, `20_job_agg_1m.sql`. |
| `flink/client-conf/config.yaml` | Tells the one-shot SQL client containers where the cluster is. |
| `db/init/` | PostgreSQL schema, applied on first start of the volume only. |
| `grafana/` | Provisioned datasource and dashboard. |
| `src/streamlab/` | Python package: generator (test oracle), producer, replay adapter, verify, CLI. |
| `tests/` | Unit tests; no Docker required. |
| `scripts/` | `smoke.sh` (end-to-end), `check_compat.py` (schema compatibility). |
| `docs/` | Architecture, decisions (ADRs), roadmap, known unknowns, interview talking points. |

## Commands

```bash
make setup         # venv + deps + .env
make test          # unit tests, no Docker (run this constantly)
make lint          # ruff check + format check
make up            # docker compose up -d --build
make gen           # send messy synthetic data, write truth.jsonl
make verify        # compare PostgreSQL agg_1m with truth.jsonl
make smoke         # gen + wait + verify
make down          # stop;  make clean  # stop AND delete volumes
```

## Conventions

* Python 3.11+, type hints on public functions, `ruff` (line length 100) must pass.
* Avro schema changes: add a new file or a candidate under `schemas/avro/evolution/`, prove
  BACKWARD compatibility (`tests/test_schemas.py`, `scripts/check_compat.py`), and record an
  ADR in `docs/DECISIONS.md`. Never edit a registered schema in place.
* Flink SQL: all columns `NOT NULL` where the data guarantees it; one job per file; set
  `pipeline.name`; keep the watermark delay in sync with `generator.WATERMARK_DELAY_S`.
* Pin versions (images, connectors, Python deps). When bumping a Flink minor version, bump the
  connector versions in `.env.example` together and note it in `docs/KNOWN-UNKNOWNS.md`.
* Dev machine is Apple Silicon (arm64) with a Docker VM of at most ~12 GB. Do not add an
  always-on service without updating the memory table in `docs/ARCHITECTURE.md`; prefer
  Compose profiles for optional components.
* Timestamps are UTC everywhere. Event time comes from the source, never from the broker.

## Verification (definition of done)

A change is done when:

1. `make lint` and `make test` pass.
2. If it touches `docker-compose.yml`, `flink/`, `db/`, `schemas/` or the producer: `make up`
   succeeds, `make smoke` prints `OK`, and the Flink UI (http://localhost:8082) shows both
   jobs RUNNING with checkpoints completing.
3. Docs that describe the changed behaviour are updated in the same change.

If you only did step 1, say "unit-tested only, stack not run".

## First things to do

The platform has **never been started** by the author of this draft (no Docker was
available). Start with tasks T-00 and T-01 in `docs/ROADMAP.md`; the likely failure points
are already listed in `docs/KNOWN-UNKNOWNS.md` with symptoms and candidate fixes.

## Common pitfalls

* Kafka from the host uses `localhost:9092`; containers use `kafka:19092`. Mixing them gives
  connections that succeed and then hang.
* Flink web UI is on host port **8082** (8081 is Schema Registry).
* `db/init/*.sql` runs only when the Postgres volume is empty; schema changes need
  `make clean` or a migration.
* Flink's Kafka sink default transaction timeout (1 h) exceeds the broker maximum (15 min);
  the SQL sets it explicitly. Do not remove that option.
* Window aggregation drops events later than the watermark; that is intentional in Phase 1
  and encoded as `counted=False` in the generator's ground truth.
