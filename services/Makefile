.PHONY: up down setup status clean help

up: ## Start Cube.js + Metabase (BigQuery)
	docker compose up -d --build
	@echo ""
	@echo "Services started:"
	@echo "  Cube.js Playground: http://localhost:4000"
	@echo "  Cube.js REST API:   http://localhost:4000/cubejs-api/v1"
	@echo "  Cube.js SQL API:    localhost:15432"

down: ## Stop all services
	docker compose down

status: ## Show running services
	docker compose ps

clean: ## Stop and remove all data (resets Metabase config)
	docker compose down -v

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
