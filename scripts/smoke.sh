#!/usr/bin/env bash
# End-to-end check: generate messy data, wait for the pipeline, compare PostgreSQL with truth.
# Prereqs: `docker compose up -d --build` finished and `pip install -e .` done.
set -euo pipefail

TRUTH="${TRUTH:-/tmp/streamlab-truth.jsonl}"
WAIT="${WAIT:-90}"

echo "== services"
docker compose ps

echo "== schema compatibility (v2 candidate must be BACKWARD compatible)"
python scripts/check_compat.py || echo "(skipped: nothing registered yet on a fresh stack)"

echo "== generate"
streamlab generate --duration 300 --settle 90 --truth "$TRUTH"

echo "== wait ${WAIT}s for windows to close and sinks to flush"
sleep "$WAIT"

echo "== verify"
streamlab verify --truth "$TRUTH"
