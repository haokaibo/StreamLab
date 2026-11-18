# Decisions (ADR log)

Format: context, decision, consequences. Add new entries at the bottom; do not rewrite old
ones, supersede them.

## ADR-001: Synthetic + replayed telemetry as the Phase 1 source
Context: no reliable external feed, no need for live market data, and real store/camera data
must not enter a public repo. Decision: a deterministic generator plus a replay adapter for
recorded host-sensor readings. Consequences: full control over duplicates, lateness and
volume; the generator doubles as a test oracle. Real-world sources arrive in later phases.

## ADR-002: Flink SQL first, Java DataStream later
Context: SQL covers dedup, event-time windows, Kafka/JDBC/file sinks with little code and no
Maven build. Decision: Phase 1 jobs are Flink SQL. Consequences: late-event side outputs,
custom state and timers are not possible in SQL; they are roadmap tasks T-05/T-06 in Java.

## ADR-003: Two jobs with an intermediate topic
Decision: job A deduplicates into `telemetry.clean.v1`; job B windows from it. Why: dedup
that keeps the first arrival (ordered by processing time) is insert-only, so it can feed an
exactly-once Kafka sink and window aggregation; event-time dedup would emit updates that
window TVFs cannot consume. Consequences: one extra topic and a second consumer lag to watch.

## ADR-004: Avro record named `record` in `org.apache.flink.avro.generated`
Context: Flink's avro-confluent format derives the reader schema from the table DDL and uses
that default record name; a differently named writer record can fail schema resolution.
Decision: use Flink's default name in `telemetry_sample.v1.avsc`. Consequences: slightly
unusual naming. Revisit with the `avro-confluent.schema` option (see KNOWN-UNKNOWNS K-06).

## ADR-005: JSON for the raw archive, Parquet later
Context: the Parquet writer needs Hadoop classes in the Flink image, a common source of
`NoClassDefFoundError`. Decision: JSON lines now. Consequences: larger files, no column
pruning; Parquet is roadmap task T-08.

## ADR-006: PostgreSQL upsert instead of exactly-once JDBC
Decision: rely on primary-key upserts for idempotency. Consequences: the sink is
at-least-once but the table content is effectively exactly-once for this key.

## ADR-007: Docker Compose first, Kubernetes second
Decision: get the platform correct under Compose, then port to a local Kubernetes cluster
(k3d/kind/OrbStack) in Phase 2. Consequences: networking and storage issues of Kubernetes
do not block pipeline development.

## ADR-008: track the newest Flink that its JDBC connector supports
Context: Flink 2.3.0 is the newest release, but `flink-connector-jdbc` stops at `4.1.0-2.2`
(no `-2.3` artifact is published). Choosing Flink purely by recency would leave job B with
no sink.
Decision: pin Flink to **2.2.1** and both connectors to their `-2.2` builds; upgrade all
three together when a 2.3 connector is released. Kafka is not bound this way, so it tracks
the newest stable (4.4.0).
Consequences: the platform is one Flink minor behind latest, and Flink 1.20 -> 2.2 is a major
jump that carried breaking config renames (`state.backend.type` -> `state.backend`) and a
split JDBC connector. Consequence for later: connector compatibility, not release date,
drives the Flink pin; record the 404 evidence in `docs/KNOWN-UNKNOWNS.md` K-14.

## ADR-009: local filesystem instead of an object store

Context: Phase 1 was written against S3-compatible storage (MinIO). By the time the stack was
first started, MinIO had retired its community images -- Docker Hub returns 404 and
`quay.io/minio/minio` returns 401 for every tag -- and there is no AWS account available for
this project.

Decision: drop the object-store layer. The raw archive and Flink's checkpoints and
savepoints use plain `file://` paths on a bind mount at `./data`, and the `minio` and
`minio-init` services are removed. `data/` is git-ignored.

Consequences:

* Two fewer services and about 512 MB of memory budget released.
* No credential handling, no bucket bootstrap, no SigV4 signing to get wrong.
* The archive is no longer reachable from another machine. Revisiting this (Garage,
  SeaweedFS or a real bucket) is the same swap in one place: the `raw_archive` path and the
  two `state.*.dir` properties.
* `sink.rolling-policy` still applies, so the archive remains replayable as JSON lines
  partitioned by `dt`/`hr`.
