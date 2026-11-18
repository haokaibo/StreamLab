.PHONY: help setup up down logs ps ui topics gen replay verify smoke test lint fmt psql clean

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*##' Makefile | awk -F':.*## ' '{printf "%-10s %s\n", $$1, $$2}'

setup: ## create venv, install deps, copy .env
	python3 -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
	@test -f .env || cp .env.example .env

up: ## build and start the platform
	docker compose up -d --build

down: ## stop the platform (keeps volumes)
	docker compose down

logs: ## follow logs of all services
	docker compose logs -f --tail=100

ps: ## service status
	docker compose ps -a

ui: ## start with Kafka UI on :8080
	docker compose --profile ui up -d

topics: ## list Kafka topics
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list

gen: ## send synthetic messy data and write ground truth
	streamlab generate --duration 300 --settle 90 --truth truth.jsonl

replay: ## replay the sample recorded data
	streamlab replay --file sample_data/hwmon_sample.jsonl

verify: ## compare PostgreSQL with ground truth
	streamlab verify --truth truth.jsonl

smoke: ## end-to-end check
	./scripts/smoke.sh

test: ## unit tests (no Docker needed)
	pytest -q

lint: ## ruff lint and format check
	ruff check . && ruff format --check .

fmt: ## auto-format
	ruff check --fix . && ruff format .

psql: ## open psql in the postgres container
	docker compose exec postgres psql -U streamlab -d streamlab

clean: ## stop and delete ALL volumes (data loss)
	docker compose down -v
