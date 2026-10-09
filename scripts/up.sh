#!/usr/bin/env bash
# Builds and starts the full stack, then waits for the API and Debezium. Use via `make up`.
set -euo pipefail
cd "$(dirname "$0")/.."

command -v docker >/dev/null || { echo "Error: docker is not installed" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Error: Docker is not running" >&2; exit 1; }

echo "Building and starting all services (first start can take 30-60 seconds)..."
docker compose up -d --build

echo "Waiting for API..."
until curl -sf http://localhost:5000/api/health >/dev/null 2>&1; do sleep 2; done
echo "Waiting for Debezium..."
until curl -sf http://localhost:8083/ >/dev/null 2>&1; do sleep 2; done

docker compose ps
cat <<MSG

Stack is up:
  Web UI:       http://localhost:8000
  API:          http://localhost:5000/api/health
  Debezium API: http://localhost:8083/connectors

Stop with 'make down' (keep data) or 'make clean' (delete data). Logs: 'make logs'.
MSG
