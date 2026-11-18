# Talking points

Use these to explain the project in an interview. Replace the "verified" placeholders with
real observations once the stack has actually run.

## 30-second pitch
A single-machine streaming platform with Kafka, Avro/Schema Registry and Flink SQL. A
deterministic generator injects duplicates, out-of-order and very late events, and a verifier
proves the database matches what was really sent.

## Questions you should be able to answer

**How do you get exactly-once?** Idempotent producer; Flink checkpoints every 10 s; Kafka sink
with transactions committed on checkpoint; consumers use `read_committed`; the database sink
is an upsert on a primary key, so replays overwrite instead of duplicating.

**How do you deduplicate, and what are the limits?** Keep-first by event id in keyed state,
ordered by processing time so the operator is insert-only. State has a TTL (2 h); duplicates
later than that slip through. Production alternative: bloom filter or external store.

**What happens to late events?** Watermark = max event time - 30 s. Events beyond it are
dropped by the SQL window operator (documented). Roadmap: Java job with a side output into a
late topic, then a batch correction.

**Why two jobs and an intermediate topic?** Event-time dedup emits updates that window
aggregations cannot consume; processing-time keep-first is append-only. Intermediate topic
also decouples consumers and can be replayed.

**How would you evolve the schema?** New optional field with a default is BACKWARD
compatible: `schemas/avro/evolution/` and `scripts/check_compat.py` prove it; the registry
rejects breaking changes. Contrast with the batch world: here the failure is prevented at
write time instead of discovered when a nightly job breaks.

**How do you replay?** Raw JSON archive by hour on local files, or rewind Kafka
offsets; event ids are deterministic (UUIDv5) for replayed sensor data, so replays dedupe.

**How do you know it is correct?** `streamlab verify` compares closed windows with the
generator's ground truth, including expected drops.

**What would you change at 100x scale?** Partition count and key design, RocksDB state backend
with incremental checkpoints, a real object store, Kubernetes with the Flink operator, lag and
checkpoint-duration alerting, compaction of small files.

## Honest limitations to volunteer
Synthetic volume of a few events per second; single broker; late-event handling is a known
gap in Phase 1; production on-call experience comes from past work, not this project.

## Links to prior batch experience (use real details)
Scheduled batch ELT with replayable raw files, idempotent weekly reloads, CDC with DMS, and
reacting to schema drift from failed jobs: this project is the streaming version of the same
problems, with the failure moved from "job fails later" to "contract enforced at write time".
