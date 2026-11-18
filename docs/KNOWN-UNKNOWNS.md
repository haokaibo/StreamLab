# Known unknowns (what was never run)

This draft was written without Docker available. Everything below is a *prediction* of where
the first `make up` can fail, with symptoms and candidate fixes. When you verify or fix an
item, change its status and add the date and what you did.

Status legend: `unverified` / `verified` / `fixed`.

| ID | Area | Risk | Symptom | Candidate fix | Status |
|---|---|---|---|---|---|
| K-01 | Images | Tags may not exist or lack linux/arm64 (`apache/kafka:3.9.0`, `cp-schema-registry:7.7.1`, Grafana, kafka-ui). | `manifest unknown` or `no matching manifest for linux/arm64` on pull. | Check the registry, bump the tag in `.env.example`. | unverified |
| K-02 | MinIO | Upstream stopped distributing community images in 2025; the pinned `quay.io/minio/minio` and `mc` tags may be gone. | Pull fails for `minio` or `minio-init`. | **Resolved by removing the S3 layer entirely** (ADR-009): archives and checkpoints now use a bind mount under `data/`. No AWS account is available for this project. | fixed |
| K-03 | Kafka | Data volume permissions for `/var/lib/kafka/data` with the `apache/kafka` image's non-root user. | Broker exits with a permission error. | Remove `KAFKA_LOG_DIRS` and the volume mount (data in the container), or chown via an init step. | unverified |
| K-04 | Flink image | Tag `flink:1.20.1-scala_2.12-java17`, or connector jar versions, may not exist (Maven Central 404 during build). | `docker compose build` fails in the `fetch` stage. | Adjust `FLINK_*` versions in `.env.example`; connector minor must match Flink minor. A newer Flink line may need newer connectors. | unverified |
| K-05 | Flink SQL client | `bin/sql-client.sh` may not pick up `flink/client-conf/config.yaml` (file name differs by Flink version), so it targets localhost:8081. | `Could not connect to Flink cluster` / connection refused in `flink-job-*` logs. | Rename to `flink-conf.yaml`, or pass `-Drest.address=flink-jobmanager`, or use the SQL Gateway. | unverified |
| K-06 | Avro | Reader-schema record-name mismatch between the producer's schema and the one Flink derives. | `Found X, expecting org.apache.flink.avro.generated.record` in job logs. | Keep ADR-004 naming, or set `avro-confluent.schema` with the exact registered schema (all fields, same order). | unverified |
| K-07 | Avro | Nullability: Flink derives `NOT NULL` columns as non-union types. Registered v1 uses non-union types, so it should match. | Deserialization errors on `unit`/`labels`. | Make the DDL columns match the .avsc exactly; add `labels MAP<STRING,STRING>` if needed. | unverified |
| K-08 | Kafka sink | Exactly-once needs `transaction.timeout.ms` <= broker max (set to 10 min) and consumers using `read_committed` (set in job B). | `InvalidTransactionTimeout`, or job B shows no data for >10 s. | Keep the explicit timeout; data appears after each checkpoint (10 s). | unverified |
| K-09 | File sink | The S3 plugin must be enabled in the client container too, or planning fails on `s3://`. | `Could not find a file system implementation for scheme 's3'` in `flink-job-clean`. | Ensure `ENABLE_BUILT_IN_PLUGINS` is honoured for the client command or copy the plugin into `lib/`. | unverified |
| K-10 | Window job | No data in `agg_1m` because the watermark never advances (idle partitions). | Job B RUNNING, table empty. | `table.exec.source.idle-timeout` is set to 10 s; check that it applies, and that the generator's settle tail is long enough. | unverified |
| K-11 | JDBC | Type mapping `TIMESTAMP(3)` to `timestamp`; upsert needs the primary key declared in both DDLs. | `column ... is of type timestamp but expression is of type ...`. | Align Postgres DDL with Flink DDL. | unverified |
| K-12 | Grafana | Provisioned datasource uses `$POSTGRES_*` environment interpolation; query variables with multi-select. | Datasource test fails or panels empty. | Inline the values, or check Grafana version's postgres datasource format. | unverified |
| K-13 | Verify | Truth vs database can differ at window edges when partitions advance watermarks at different speeds. | `verify` reports a few count mismatches in the newest closed windows. | Increase `margin_s` in `expected_aggregates`, or lengthen `--settle`. | unverified |

## Already verified (unit level, no Docker)

* Avro schemas parse; v2 candidate reads v1 data with defaults (backward compatible).
* Generator is deterministic; duplicates, out-of-order, late and bad records all occur; ground
  truth is consistent; expected aggregates match a brute-force computation.
* Producer-side validation rejects NaN/Inf/empty keys and naive timestamps.
* The Avro serializer produces the Confluent wire format, registers subject
  `telemetry.raw.v1-value`, and round-trips `timestamp-millis` values (against confluent-kafka's
  in-memory mock registry, not a real Schema Registry).
* Replay produces valid events with stable ids (idempotent) and inferred units.
* Compose/SQL/Postgres consistency: topics, credentials, watermark/window constants, memory
  budget, ports, dependencies, dashboard datasource.

## Not covered by any test yet

Kafka and Flink behaviour, the real Schema Registry (compatibility rules), JDBC upserts,
checkpoint recovery, Grafana rendering.

## Dependency upgrade 2026-11 (unverified)

All pins were moved to their current stable releases except Flink, which is capped by its
JDBC connector. Nothing below was run against a live stack; the sandbox used for the change
could not start Docker.

| Component | From | To | Note |
|---|---|---|---|
| apache/kafka | 3.9.0 | **4.4.0** | KRaft-only since 4.0; ZooKeeper mode is gone |
| cp-schema-registry | 7.7.1 | 7.9.11 | |
| postgres | 17 | 17.12 | same major, data volume compatible |
| grafana | 11.6.0 | 13.3.1 | dashboard JSON may need touch-ups |
| postgres JDBC | 42.7.4 | 42.7.7 | |
| Flink | 1.20.1 | **2.2.1** | newest usable; 2.3.0 has no JDBC connector |
| Kafka connector | 3.4.0-1.20 | 5.0.0-2.2 | |
| JDBC connector | 3.3.0-1.20 | 4.1.0-2.2 | split into `jdbc-core` + `jdbc-postgres` |
| Python libs | lower bounds | current stable | `>=` ranges, resolves to latest |

### What had to change for Kafka 4.x

* `KAFKA_CONTROLLER_QUORUM_VOTERS` was **removed** in favour of
  `KAFKA_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS`. Kafka 4.0 deprecated `voters`; a dynamic
  quorum must not set it. Keeping the old key formats a static quorum and the broker refuses
  to start.

### What had to change for Flink 2.x

* `state.backend.type` was **removed** in 2.0; the key is now `state.backend`. Leaving the
  old key in `FLINK_PROPERTIES` is a startup failure.
* Flink 2.x reads only `config.yaml`; `flink-conf.yaml` no longer exists. This resolves
  K-05 in our favour, but the SQL-client mount path
  (`/opt/flink/conf/config.yaml`) still needs a real run to confirm.
* The JDBC connector no longer ships one shaded jar for the SQL client: 4.x needs
  `flink-connector-jdbc-core` **plus** `flink-connector-jdbc-postgres`. `flink/Dockerfile`
  now fetches both.
* Connector minor versions must match the Flink minor; they were bumped together.

| ID | Area | Risk | Symptom | Candidate fix | Status |
|---|---|---|---|---|---|
| K-14 | Flink | Flink 2.3.0 exists but `flink-connector-jdbc` has no `-2.3` build (verified 404 on Maven Central). | Job B cannot plan: no JDBC sink implementation. | Stay on 2.2.1; bump Flink and both connectors together once a 2.3 connector ships. | known limit |
| K-15 | Kafka | Kafka 4.x images may default `log.dirs` and reject the non-root user on the mounted volume (extends K-03). | Broker exits with a permission error. | Same as K-03: drop `KAFKA_LOG_DIRS`/volume, or chown in an init step. | unverified |
| K-16 | MinIO | The pinned community image may no longer be pullable (K-02 still stands, unchanged by this upgrade). | Pull fails for `minio`/`minio-init`. | Superseded: both services were removed, see ADR-009. | fixed |


## T-00 bring-up results (2026-11-18, actually run)

`make up` now brings every service to healthy/completed and both jobs to RUNNING. What broke
on the first real start, and what fixed it:

| ID | Symptom | Cause | Fix | Status |
|---|---|---|---|---|
| K-02 | `quay.io/minio/minio` returned **401 UNAUTHORIZED** on every tag; Docker Hub returns 404. | MinIO retired its community images. | Dropped the object store entirely; archives and checkpoints use `file://` on a bind mount (ADR-009). | fixed |
| K-17 | Kafka exited 1: `Because controller.quorum.voters is not set on this controller, you must specify one of --standalone, --initial-controllers, --no-initial-controllers`. | The 4.x upgrade had replaced `voters` with `bootstrap.servers`, but the image wrapper only formats a *static* quorum. | Restored `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093`; the image's own comment says it keeps static voters on purpose. | fixed |
| K-18 | Both jobs failed: `UnsupportedOperationException: Unsupported to derive Schema for type: TIMESTAMP_LTZ(3)`. | `avro-confluent` uses Avro's legacy timestamp mapping by default, which cannot map `TIMESTAMP_LTZ`. The `avro.timestamp_mapping.legacy` option is not on the confluent format's option whitelist. | Declared `event_time` as `TIMESTAMP(3)`; with `table.local-time-zone=UTC` it carries a UTC wall clock matching the schema's `timestamp-millis`. | fixed |
| K-19 | Job B failed: `ClassNotFoundException: io.openlineage.client.utils.jdbc.JdbcExtractor`. | The 4.x JDBC connector links against OpenLineage but does not bundle it. | `flink/Dockerfile` now also fetches `openlineage-java` and `openlineage-sql-java` **1.32.0**, the version in `flink-connector-jdbc-parent`. | fixed |
| K-20 | `make verify` failed with `role "streamlab" does not exist` even though the role exists. | A local Postgres already listened on host `127.0.0.1:5432`, so the CLI reached the wrong server. | Moved the published port to **55432** (`55432:5432`) and pointed `DEFAULT_DSN` at it. | fixed |

### Verified end to end

* `docker compose ps`: flink-jobmanager, kafka, schema-registry, postgres healthy; one-shot
  job containers exit 0.
* `http://localhost:8082/jobs/overview`: `streamlab-clean` RUNNING, `streamlab-agg-1m` RUNNING.
* Checkpoints: 12 completed / 0 failed on both jobs.
* `streamlab generate --duration 60 --settle 40`: sent 811, dead-lettered 1, 0 delivery errors.
* `streamlab verify`: `OK: database aggregates match ground truth for all closed windows`.
* `agg_1m` holds 8 rows (2 hosts x 4 metrics) with `sample_count = 42` each.
* Raw archive written to `data/raw-archive/telemetry/dt=2026-11-18/hr=01/`.
* Grafana health endpoint returns 200.

Still unverified: Grafana panel rendering (T-03) and TaskManager kill/restart
recovery (T-04).

## T-01 end-to-end proof (2026-11-18, actually run)

`make smoke` prints OK with default parameters and with `--dup 0.2 --ooo 0.3`. Two real
defects surfaced while making it pass:

| ID | Symptom | Cause | Fix | Status |
|---|---|---|---|---|
| K-21a | `verify` disagreed: `telemetry.clean.v1` held ~20 000 records across several runs and job B (which reads `earliest-offset`) fed stale windows into `agg_1m`, so the last upserted window's count was wrong (e.g. 99 where 60 were expected). | `smoke.sh` only `TRUNCATE`d the table and never cleared the Kafka topics, so every run layered on the previous one. | `smoke.sh` now truncates `agg_1m` **and** clears the archive before each run, and the stack is expected to be reset (fresh topics) before a proof run. A fresh-seed run is isolated; see K-21b. | fixed |
| K-21b | `verify` FAILed with `count mismatch ... got 60, expected 59` on the final window(s) even on a clean stack. | `event_id` is derived from the seed alone, so a run reuses the ids of any earlier run with the same seed; the *stateful* dedup job drops the re-sent records against its old RocksDB state. Separately, the burst mode sends every record at once, so a `late` event (emitted 120–240 s late) is still counted because the watermark is driven by `event_time`, not wall time -- contradicting the generator's ground truth. | `smoke.sh` uses a fresh `$RANDOM` seed per run **and** `--mode realtime`, so late events genuinely arrive past the watermark and the truth matches the pipeline. `--duration`/`--settle`/`--wait` are now env-tunable (defaults 120/90/90). | fixed |

### Verified

* Run 1 (default): `sent=1703 dead_lettered=3`, `verify` → `OK: database aggregates match
  ground truth for all closed windows`.
* Run 2 (`--dup 0.2 --ooo 0.3`): `sent=1698 dead_lettered=5`, `verify` → `OK`.

Note: `make smoke -- --dup 0.2 --ooo 0.3` prints a trailing `make: No rule to make target
'--dup'` warning. That is make parsing the extra args after the target; the smoke script
still runs to completion and prints OK. Harmless, cosmetic.
