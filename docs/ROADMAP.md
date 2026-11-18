# Roadmap

Tasks are sized for one agent session each. Update status here when done. "Verify" means run
for real, not just unit tests.

## Phase 1: platform and pipeline (current)

- [x] **T-00 Bring-up.** `make up` brings every service to healthy/completed on arm64.
  Accept: `docker compose ps` clean; both Flink jobs RUNNING in http://localhost:8082.
  Start with `docs/KNOWN-UNKNOWNS.md`.
- [x] **T-01 End-to-end proof.** `make smoke` prints OK (database matches ground truth) with
  default parameters, then with `--dup 0.2 --ooo 0.3`. Fix whatever breaks; record every
  fix in KNOWN-UNKNOWNS. (Done 2026-11-18; fixes K-21a/K-21b -- smoke now uses realtime
  mode and a fresh seed, and clears state between runs.)
- [ ] **T-02 Real sensor replay.** Adapt `src/streamlab/replay.py` to the real hw_monitor.py
  output, using a sanitised sample. Accept: replaying twice yields the same rows in `agg_1m`.
- [ ] **T-03 Dashboard check.** Grafana "streamlab overview" shows data after `make smoke`;
  fix queries if not.
- [ ] **T-04 Restart safety.** Kill the TaskManager mid-run (`docker compose kill
  flink-taskmanager`), restart it, and show `verify` still passes (checkpoint recovery).
  Write the result in `docs/TALKING-POINTS.md`.

## Phase 1.5: depth

- [ ] **T-05 Late events side output.** Java (Maven, Flink DataStream) job that windows like
  job B but routes late events to a topic `telemetry.late.v1`. Accept: ground-truth `late`
  events all appear there.
- [ ] **T-06 Timer-based job.** Alert when a metric stays above a threshold for N minutes
  (keyed state + timers). Accept: unit test with Flink test harness plus an alert topic.
- [ ] **T-07 Schema evolution drill.** Register v2 candidate, run old producer and new
  consumer together, show Schema Registry rejecting an incompatible change.
- [ ] **T-08 Parquet archive.** Switch `raw_archive` to Parquet; solve Hadoop classpath.
- [ ] **T-09 Batch reprocessing.** A Python/dbt job that rebuilds `agg_1m` from the archive and
  a reconciliation report that diffs it against the streaming result.
- [ ] **T-10 Data quality.** Per-minute completeness/uniqueness/timeliness metrics table and
  alert thresholds.

## Phase 2: platform

- [ ] **T-11 Kubernetes.** Run on a local cluster with Helm (Kafka, Flink Kubernetes
  Operator, PostgreSQL, an object store). Add a Compose-vs-K8s comparison note.
- [ ] **T-12 Observability.** Prometheus + Grafana for Kafka lag and Flink metrics; alert
  rules.
- [ ] **T-13 CI e2e.** Make the manual `e2e` GitHub Actions job reliable.

## Phase 3: real sources (later)

Store event metadata (anonymous counts/zones only) and a stream-vs-batch reconciliation with
exported bookkeeping data. Only sanitised samples ever enter this repo.
