SHELL := /bin/bash

.DEFAULT_GOAL := help

.PHONY: help modules fmt fmt-check tidy tidy-check test test-integration vet lint check

help: ## Show available targets
	@echo "Available targets:"
	@echo "  modules          List independent Go modules"
	@echo "  fmt              Format Go files in every module"
	@echo "  fmt-check        Verify Go formatting"
	@echo "  tidy             Tidy dependencies in every module"
	@echo "  tidy-check       Verify committed go.mod and go.sum files"
	@echo "  test             Run tests with the race detector"
	@echo "  test-integration Run integration tests in every tests/integration module (requires Docker)"
	@echo "  vet              Run go vet in every module"
	@echo "  lint             Run golangci-lint in every module"
	@echo "  check            Run all required validation"

modules: ## List independent Go modules
	@./scripts/modules.sh

fmt: ## Format Go files in every module
	@./scripts/fmt.sh

fmt-check: ## Verify Go formatting
	@./scripts/fmt-check.sh

tidy: ## Tidy dependencies in every module
	@./scripts/tidy.sh

tidy-check: ## Verify committed go.mod and go.sum files
	@./scripts/tidy-check.sh

test: ## Run tests with the race detector
	@./scripts/test.sh

test-integration: ## Run integration tests in every tests/integration module (requires Docker)
	@./scripts/test-integration.sh

vet: ## Run go vet in every module
	@./scripts/vet.sh

lint: ## Run golangci-lint in every module
	@./scripts/lint.sh

check: fmt-check tidy-check test vet lint ## Run all required validation
