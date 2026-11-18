#!/usr/bin/env bash
# End-to-end check: generate messy data, wait for the pipeline, compare PostgreSQL with truth.
# Prereqs: `docker compose up -d --build` finished and `pip install -e .` done.
set -euo pipefail

TRUTH="${TRUTH:-/tmp/streamlab-truth.jsonl}"
# In realtime mode the run lasts (DURATION + SETTLE) wall-clock seconds, so WAIT only needs
# to cover the 30 s watermark delay plus sink flushing (see K-21).
DURATION="${DURATION:-120}"
SETTLE="${SETTLE:-90}"
WAIT="${WAIT:-90}"
# A fresh seed per run makes every event_id unique. event_id is derived from the seed
# alone, so two runs with the same seed would reuse ids and the *stateful* dedup job would
# drop the re-sent records as duplicates against its stale state (K-21).
SEED="${SEED:-$RANDOM$RANDOM}"
# Any arguments after the target are forwarded to `streamlab generate`, e.g.
#   make smoke -- --dup 0.2 --ooo 0.3
GEN_ARGS=("$@")

echo "== services"
docker compose ps

echo "== schema compatibility (v2 candidate must be BACKWARD compatible)"
python scripts/check_compat.py || echo "(skipped: nothing registered yet on a fresh stack)"

echo "== isolate: clear aggregates and archive so verify only sees this run"
docker compose exec -T postgres psql -U streamlab -d streamlab -c "TRUNCATE agg_1m;"
rm -rf data/raw-archive/telemetry/*

# --mode realtime is required: the generator's ground truth assumes a late event is dropped
# only when its arrival (event_time + 120..240 s) is genuinely past the watermark. In burst
# mode everything lands at once, the watermark is driven by event_time, and late events get
# counted, so the database would disagree with the truth (K-21).
echo "== generate (seed=$SEED, realtime, ${DURATION}s + ${SETTLE}s settle)"
if [ ${#GEN_ARGS[@]} -gt 0 ]; then
  streamlab generate --duration "$DURATION" --settle "$SETTLE" --mode realtime --seed "$SEED" --truth "$TRUTH" "${GEN_ARGS[@]}"
else
  streamlab generate --duration "$DURATION" --settle "$SETTLE" --mode realtime --seed "$SEED" --truth "$TRUTH"
fi

echo "== wait ${WAIT}s for windows to close and sinks to flush"
sleep "$WAIT"

echo "== verify"
streamlab verify --truth "$TRUTH"
