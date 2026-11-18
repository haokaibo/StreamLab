-- Job A: telemetry.raw.v1 -> deduplicate by event_id -> telemetry.clean.v1
--        and, in the same job, archive every raw event (duplicates included) as JSON on
--        the local filesystem so any downstream job can be replayed from files.
--
-- Guarantees: Kafka sink is exactly-once (transactions committed on checkpoint); dedup
-- keeps the first arrival per event_id for as long as the state TTL below.

SET 'pipeline.name' = 'streamlab-clean';
SET 'execution.checkpointing.interval' = '10 s';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.state.ttl' = '2 h';
SET 'table.exec.source.idle-timeout' = '10 s';
-- NOTE: event_time is TIMESTAMP(3), not TIMESTAMP_LTZ. The avro-confluent format exposes
-- no timestamp-mapping option, and its legacy mapping cannot convert TIMESTAMP_LTZ to
-- an Avro schema ("Unsupported to derive Schema for type: TIMESTAMP_LTZ(3)"). With
-- table.local-time-zone=UTC the column carries a UTC wall clock, which matches the
-- schema's timestamp-millis. See docs/KNOWN-UNKNOWNS.md K-18.

CREATE TABLE telemetry_raw (
  event_id   STRING NOT NULL,
  event_time TIMESTAMP(3) NOT NULL,
  host       STRING NOT NULL,
  source     STRING NOT NULL,
  metric     STRING NOT NULL,
  `value`    DOUBLE NOT NULL,
  unit       STRING NOT NULL,
  seq        BIGINT NOT NULL,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'telemetry.raw.v1',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'streamlab-clean',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE telemetry_clean_sink (
  event_id   STRING NOT NULL,
  event_time TIMESTAMP(3) NOT NULL,
  host       STRING NOT NULL,
  source     STRING NOT NULL,
  metric     STRING NOT NULL,
  `value`    DOUBLE NOT NULL,
  unit       STRING NOT NULL,
  seq        BIGINT NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = 'telemetry.clean.v1',
  'properties.bootstrap.servers' = 'kafka:19092',
  -- must be <= broker transaction.max.timeout.ms (15 min); Flink's default of 1 h is rejected
  'properties.transaction.timeout.ms' = '600000',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'streamlab-clean',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081',
  'avro-confluent.subject' = 'telemetry.clean.v1-value'
);

CREATE TABLE raw_archive (
  event_id   STRING,
  event_time TIMESTAMP(3),
  host       STRING,
  source     STRING,
  metric     STRING,
  `value`    DOUBLE,
  unit       STRING,
  seq        BIGINT,
  dt         STRING,
  hr         STRING
) PARTITIONED BY (dt, hr) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///opt/streamlab/data/raw-archive/telemetry/',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601',
  'sink.rolling-policy.rollover-interval' = '1 min',
  'sink.rolling-policy.check-interval' = '10 s'
);

-- Keep the first arrival of each event_id. Ordering by processing time makes this an
-- insert-only operator (no retractions), which the Kafka sink requires.
CREATE TEMPORARY VIEW telemetry_dedup AS
SELECT event_id, event_time, host, source, metric, `value`, unit, seq
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS rn
  FROM telemetry_raw
)
WHERE rn = 1;

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO telemetry_clean_sink
  SELECT event_id, event_time, host, source, metric, `value`, unit, seq
  FROM telemetry_dedup;

  INSERT INTO raw_archive
  SELECT event_id, event_time, host, source, metric, `value`, unit, seq,
         DATE_FORMAT(CAST(event_time AS TIMESTAMP(3)), 'yyyy-MM-dd'),
         DATE_FORMAT(CAST(event_time AS TIMESTAMP(3)), 'HH')
  FROM telemetry_raw;
END;
