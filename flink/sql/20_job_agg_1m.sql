-- Job B: telemetry.clean.v1 -> 1-minute event-time tumbling windows per (host, metric)
--        -> PostgreSQL agg_1m (idempotent upsert on the primary key).
--
-- Events later than the 30 s watermark delay are dropped by the window operator. That is a
-- known Phase 1 limitation; capturing them in a side output is roadmap task T-05.
-- WATERMARK_DELAY_S in src/streamlab/generator.py must stay in sync with the delay below.

SET 'pipeline.name' = 'streamlab-agg-1m';
SET 'execution.checkpointing.interval' = '10 s';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '10 s';

CREATE TABLE telemetry_clean (
  event_id   STRING NOT NULL,
  event_time TIMESTAMP_LTZ(3) NOT NULL,
  host       STRING NOT NULL,
  source     STRING NOT NULL,
  metric     STRING NOT NULL,
  `value`    DOUBLE NOT NULL,
  unit       STRING NOT NULL,
  seq        BIGINT NOT NULL,
  WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'telemetry.clean.v1',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'streamlab-agg-1m',
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8081'
);

CREATE TABLE agg_1m (
  window_start TIMESTAMP(3) NOT NULL,
  window_end   TIMESTAMP(3) NOT NULL,
  host         STRING NOT NULL,
  metric       STRING NOT NULL,
  sample_count BIGINT NOT NULL,
  avg_value    DOUBLE NOT NULL,
  min_value    DOUBLE NOT NULL,
  max_value    DOUBLE NOT NULL,
  PRIMARY KEY (window_start, host, metric) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/streamlab',
  'table-name' = 'agg_1m',
  'username' = 'streamlab',
  'password' = 'streamlab_dev',
  'sink.buffer-flush.interval' = '1 s',
  'sink.buffer-flush.max-rows' = '200'
);

INSERT INTO agg_1m
SELECT
  window_start,
  window_end,
  host,
  metric,
  COUNT(*)      AS sample_count,
  AVG(`value`)  AS avg_value,
  MIN(`value`)  AS min_value,
  MAX(`value`)  AS max_value
FROM TABLE(TUMBLE(TABLE telemetry_clean, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, host, metric;
