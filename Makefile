.DEFAULT_GOAL := help

.PHONY: env
env: ## Start env
	docker compose -p wkafka -f env/docker-compose.yml up -d --remove-orphans

.PHONY: env-up
env-up: ## Start env without detaching
	docker compose -p wkafka -f env/docker-compose.yml up --remove-orphans

.PHONY: env-logs
env-logs: ## Show env logs
	docker compose -p wkafka logs -f

.PHONY: env-down
env-down: ## Stop env
	docker compose -p wkafka down

.PHONY: example
example: LOG_LEVEL ?= debug
example: ## Run example
	cd example && LOG_LEVEL=$(LOG_LEVEL) go run ./main.go

.PHONY: ci-run
ci-run: ## Run CI in local with act
	act -j sonarcloud

.PHONY: lint
lint: ## Lint Go files
	golangci-lint--version
	GOPATH="$(shell dirname $(PWD))" golangci-lint run ./...

.PHONY: test
test: ## Run unit tests
	@go test -v -race ./...

.PHONY: test-short
test-short: ## Run unit tests short
	@go test -v -race -short ./...

.PHONY: test-without-cache
test-without-cache: ## Run unit tests without cache
	@go test -count=1 -v -race ./...

.PHONY: coverage
coverage: ## Run unit tests with coverage
	@go test -v -race -cover -coverpkg=./... -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out

.PHONY: help
help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
