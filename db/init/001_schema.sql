-- Serving schema. Created on first start of the postgres container.
-- All timestamps are UTC (Flink jobs run with table.local-time-zone = UTC).

CREATE TABLE IF NOT EXISTS dim_metric (
  metric      text PRIMARY KEY,
  unit        text NOT NULL,
  description text NOT NULL
);

INSERT INTO dim_metric (metric, unit, description) VALUES
  ('gpu.junction_temp_c', 'C', 'GPU junction (hotspot) temperature'),
  ('gpu.vram_temp_c',     'C', 'GPU memory temperature'),
  ('gpu.power_w',         'W', 'GPU board power draw'),
  ('cpu.package_temp_c',  'C', 'CPU package temperature')
ON CONFLICT (metric) DO NOTHING;

-- Written by Flink job B via JDBC upsert (INSERT ... ON CONFLICT on the primary key),
-- so re-running or replaying a window overwrites it instead of duplicating it.
CREATE TABLE IF NOT EXISTS agg_1m (
  window_start timestamp        NOT NULL,
  window_end   timestamp        NOT NULL,
  host         text             NOT NULL,
  metric       text             NOT NULL,
  sample_count bigint           NOT NULL,
  avg_value    double precision NOT NULL,
  min_value    double precision NOT NULL,
  max_value    double precision NOT NULL,
  inserted_at  timestamptz      NOT NULL DEFAULT now(),
  PRIMARY KEY (window_start, host, metric)
);

CREATE INDEX IF NOT EXISTS agg_1m_metric_time_idx ON agg_1m (metric, window_start DESC);

CREATE OR REPLACE VIEW agg_1m_enriched AS
SELECT a.*, d.unit, d.description
FROM agg_1m a
LEFT JOIN dim_metric d USING (metric);
