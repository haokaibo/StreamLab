# streamlab

A single-machine practice platform for production streaming concerns: **Kafka (Avro +
Schema Registry) → Flink SQL → PostgreSQL / S3-compatible storage → Grafana**, with a
deterministic data generator that doubles as a correctness oracle.

> Status: Phase 1 draft. Python code and static checks are unit-tested (`make test`).
> The Docker stack has **not been started yet**; start with `docs/ROADMAP.md` task T-00 and
> the checklist in `docs/KNOWN-UNKNOWNS.md`.

## What it demonstrates

* Event-time processing with watermarks; out-of-order and late events handled by policy.
* Deduplication by event id (at-least-once producers, effectively-once results).
* Exactly-once Kafka sink (transactions + checkpoints) and idempotent PostgreSQL upserts.
* Avro schemas in a registry with BACKWARD compatibility checks.
* Replayable raw layer on S3-compatible storage; dead-letter topic for bad records.
* Ground-truth verification: `streamlab verify` proves the database matches what the
  generator knows it sent, duplicates and lateness included.

## Quick start (Apple Silicon or Linux, Docker with ~10 GB available)

```bash
make setup                 # venv, dependencies, .env
source .venv/bin/activate
make up                    # build Flink image, start everything
make smoke                 # generate messy data, wait, verify against ground truth
```

UIs: Flink http://localhost:8082 · Grafana http://localhost:3000 (admin/admin) ·
MinIO http://localhost:9001 · Kafka UI `make ui` → http://localhost:8080.

Without Docker: `make test` runs the unit tests (generator, schemas, validation, replay,
verification logic, and static consistency checks of the infrastructure files).

## Documentation

* `docs/ARCHITECTURE.md`: data flow, topics, guarantees, memory budget.
* `docs/DECISIONS.md`: why things are the way they are (ADRs).
* `docs/ROADMAP.md`: phases and agent-sized tasks with acceptance criteria.
* `docs/KNOWN-UNKNOWNS.md`: what is unverified and how to debug it.
* `docs/TALKING-POINTS.md`: how to explain the design in an interview.
* `AGENTS.md`: rules for AI coding agents.

## Data and privacy

Phase 1 uses synthetic data and sanitised replays only. The repository must never contain
real store, camera, customer or personal data.
