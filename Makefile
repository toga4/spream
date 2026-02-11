.PHONY: lint fmt test

GOLANGCI_LINT_VERSION := v2.9.0

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

fmt:
	go fmt ./...

test:
	go test -v ./...
