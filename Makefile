LOCAL_BIN_DIR := $(PWD)/bin

## golangci configuration
GOLANGCI_CONFIG_URL   := https://raw.githubusercontent.com/worldline-go/guide/main/lint/.golangci.yml
GOLANGCI_LINT_VERSION := v1.55.2

.DEFAULT_GOAL := help

.PHONY: env
env: ## Start env
	docker compose -p wkafka -f env/docker-compose.yml up -d

.PHONY: env-up
env-up: ## Start env without detaching
	docker compose -p wkafka -f env/docker-compose.yml up

.PHONY: env-logs
env-logs: ## Show env logs
	docker compose -p wkafka logs -f

.PHONY: env-down
env-down: ## Stop env
	docker compose -p wkafka down

.PHONY: run-example
run-example: LOG_LEVEL ?= debug
run-example: ## Run example
	LOG_LEVEL=$(LOG_LEVEL) go run ./example/main.go

.PHONY: ci-run
ci-run: ## Run CI in local with act
	act -j sonarcloud

.golangci.yml:
	@$(MAKE) golangci

.PHONY: golangci
golangci: ## Download .golangci.yml file
	@curl --insecure -o .golangci.yml -L'#' $(GOLANGCI_CONFIG_URL)

bin/golangci-lint-$(GOLANGCI_LINT_VERSION):
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LOCAL_BIN_DIR) $(GOLANGCI_LINT_VERSION)
	@mv $(LOCAL_BIN_DIR)/golangci-lint $(LOCAL_BIN_DIR)/golangci-lint-$(GOLANGCI_LINT_VERSION)

.PHONY: lint
lint: .golangci.yml bin/golangci-lint-$(GOLANGCI_LINT_VERSION) ## Lint Go files
	@$(LOCAL_BIN_DIR)/golangci-lint-$(GOLANGCI_LINT_VERSION) --version
	@GOPATH="$(shell dirname $(PWD))" $(LOCAL_BIN_DIR)/golangci-lint-$(GOLANGCI_LINT_VERSION) run ./...

.PHONY: test
test: ## Run unit tests
	@go test -v -race ./...

.PHONY: test-without-cache
test-without-cache: ## Run unit tests without cache
	@go test -count=1 -v -race ./...

.PHONY: coverage
coverage: ## Run unit tests with coverage
	@go test -v -race -cover -coverpkg=./... -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -func=coverage.out

.PHONY: html
html: ## Show html coverage result
	@go tool cover -html=./coverage.out

.PHONY: html-gen
html-gen: ## Export html coverage result
	@go tool cover -html=./coverage.out -o ./coverage.html

.PHONY: html-wsl
html-wsl: html-gen ## Open html coverage result in wsl
	@explorer.exe `wslpath -w ./coverage.html` || true

.PHONY: help
help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
