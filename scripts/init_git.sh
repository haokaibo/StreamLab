#!/usr/bin/env bash
# One-shot repository bootstrap.
#
# The agent sandbox blocks creating .git inside the workspace, so this script is meant to be
# run by a human from a normal terminal:  bash scripts/init_git.sh
#
# It initialises the repository on `main` and replays the history as a series of small,
# single-concern commits (AGENTS.md ground rule 3). Each step stages one group of paths and
# commits it, so every commit is a real, working intermediate state.
#
# Idempotent: re-running on an already-initialised repository does nothing.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ -d .git ]; then
  echo "a .git directory already exists; nothing to do"
  echo "if it is an empty/broken leftover from a sandboxed attempt, remove it first:"
  echo "  rm -rf .git"
  exit 1
fi
git init -b main
git config --local core.autocrlf false
git config --local pull.rebase true
git config --local init.defaultBranch main

commit() {
  local msg="$1"
  shift
  git add -A -- "$@"
  # A path already captured by an earlier commit stages nothing; skip instead of aborting,
  # otherwise `set -e` would kill the rest of the history.
  if git diff --cached --quiet; then
    echo "  skipped (no changes): $msg"
    return 0
  fi
  git commit -q -m "$msg"
  echo "  committed: $msg"
}

# --- 1. repository plumbing ---------------------------------------------------------------
commit "chore: initialise repository with .gitignore" \
  .gitignore

# --- 2. agent and contributor rules ------------------------------------------------------
commit "docs: add agent ground rules and contributor entry point" \
  AGENTS.md CLAUDE.md

# --- 3. dependency upgrade ----------------------------------------------------------------
# Landed before the code it affects so the initial import already reflects the current pins.
commit "chore(deps): upgrade Kafka to 4.4.0 and Flink to 2.2.1" \
  .env.example flink/Dockerfile pyproject.toml \
  docs/KNOWN-UNKNOWNS.md docs/DECISIONS.md tests/test_compose.py

# --- 4. contracts: Avro schemas, Postgres DDL, Flink SQL ---------------------------------
commit "feat(schemas): add telemetry Avro v1 and BACKWARD-compatible v2 candidate" \
  schemas/

commit "feat(flink): add session-cluster image and the two SQL jobs" \
  flink/

commit "feat(db): add serving schema for agg_1m and dim_metric" \
  db/

# --- 5. platform --------------------------------------------------------------------------
commit "feat(compose): add the Phase 1 platform with a per-service memory budget" \
  docker-compose.yml

commit "feat(grafana): provision the Postgres datasource and overview dashboard" \
  grafana/

# --- 6. Python package --------------------------------------------------------------------
commit "feat(generator): add deterministic synthetic telemetry that doubles as the oracle" \
  src/streamlab/__init__.py src/streamlab/generator.py src/streamlab/truth.py

commit "feat(producer): add Avro producer with validation and a dead-letter path" \
  src/streamlab/schemas.py src/streamlab/kafka_io.py src/streamlab/validate.py

commit "feat(replay): add idempotent replay of recorded host-sensor readings" \
  src/streamlab/replay.py

commit "feat(verify): compare agg_1m against the generator ground truth" \
  src/streamlab/verify.py

commit "feat(cli): add the streamlab command line entry point" \
  src/streamlab/cli.py

# --- 7. tooling, tests, sample data ------------------------------------------------------
commit "chore(scripts): add end-to-end smoke test, schema compatibility check and git bootstrap" \
  scripts/check_compat.py scripts/smoke.sh scripts/init_git.sh

commit "test: cover generator, schemas, validation, replay, verify and infra consistency" \
  tests/ sample_data/

commit "ci: add ruff, pytest and compose-config jobs with a manual e2e run" \
  .github/

# --- 8. documentation ---------------------------------------------------------------------
commit "docs: add architecture, decision log, roadmap, known unknowns and talking points" \
  docs/ README.md

# Anything the grouping above missed (new files added later) lands here.
if [ -n "$(git status --porcelain)" ]; then
  commit "chore: add remaining files"
fi

echo
echo "== history"
git log --oneline
echo
echo "== status"
git status --short --branch
