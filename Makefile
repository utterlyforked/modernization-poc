# One interface for the compose stack. Run `make help` for targets.
.DEFAULT_GOAL := help
.PHONY: help up down clean restart logs ps debug test test-unit test-dbt test-e2e test-resilience

help: ## Show this help
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

up: ## Build and start the stack, wait until it is ready
	./scripts/up.sh

down: ## Stop the stack (keeps data volumes)
	docker compose down --remove-orphans

clean: ## Stop the stack and delete its data volumes
	docker compose down -v --remove-orphans

restart: down up ## Stop then start the stack

logs: ## Follow logs (SERVICE=cdc-consumer to pick one)
	docker compose logs -f $(SERVICE)

ps: ## Show service status
	docker compose ps

debug: ## Print a diagnostic report for the running stack
	./scripts/debug.sh

test: ## Run every suite in Docker; runs all of them and fails if any failed
	@fail=0; for t in test-unit test-dbt test-e2e test-resilience; do $(MAKE) --no-print-directory $$t || fail=1; done; exit $$fail

test-unit: ## Unit tests (no stack or network needed)
	./scripts/test-unit.sh

test-dbt: ## dbt model tests (isolated throwaway Postgres)
	./scripts/test-dbt.sh

test-e2e: ## End-to-end tests (starts and tears down its own stack)
	./scripts/test-e2e.sh

test-resilience: ## Resilience tests: stops containers mid-flight (own stack, torn down after)
	./scripts/test-resilience.sh
