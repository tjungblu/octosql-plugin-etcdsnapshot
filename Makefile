# go option
GO           ?= go
TAGS         :=
TESTS        := ./...
TESTFLAGS    :=
LDFLAGS      :=
GOFLAGS      :=
BINARY       := octosql-plugin-etcdsnapshot
MCP_BINARY   := etcdsnapshot-mcp-server
VERSION      := 0.1.6
VVERSION      := "v$(VERSION)"
OCTOSQLPATH  := ${HOME}/.octosql/plugins/etcdsnapshot/octosql-plugin-etcdsnapshot/${VERSION}/
BIN_PATH     := /usr/local/bin/

# Required for globs to work correctly
SHELL=/bin/bash

.DEFAULT_GOAL := build-all

.PHONY: release
release:
	@echo
	@echo "==> Preparing the release $(VERSION) <=="
	git tag ${VVERSION}
	git push --tags

.PHONY: build
build:
	$(GO) build -o ${BINARY} cmd/plugin/main.go

.PHONY: build-mcp
build-mcp:
	$(GO) build -o ${MCP_BINARY} cmd/mcp-server/main.go

.PHONY: build-all
build-all: build build-mcp

.PHONY: install
install: build-all
	mkdir -p ${OCTOSQLPATH}
	cp ${BINARY} ${OCTOSQLPATH}
	sudo cp ${MCP_BINARY} ${BIN_PATH}

.PHONY: test
test:
	@echo
	@echo "==> Running tests <=="
	$(GO) test $(GOFLAGS) $(TESTS) $(TESTFLAGS)

