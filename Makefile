# go option
GO           ?= go
TAGS         :=
TESTS        := ./...
TESTFLAGS    :=
LDFLAGS      :=
GOFLAGS      :=
BINARY       := octosql-plugin-etcdsnapshot
VERSION      := 0.1.5
VVERSION      := "v$(VERSION)"
OCTOSQLPATH  := ${HOME}/.octosql/plugins/etcdsnapshot/octosql-plugin-etcdsnapshot/${VERSION}/

# Required for globs to work correctly
SHELL=/bin/bash

.DEFAULT_GOAL := unit-test build

.PHONY: release
release:
	@echo
	@echo "==> Preparing the release $(VERSION) <=="
	git tag ${VVERSION}
	git push --tags

.PHONY: build
build:
	$(GO) build -o ${BINARY} main.go

.PHONY: install
install: build
	mkdir -p ${OCTOSQLPATH}
	cp ${BINARY} ${OCTOSQLPATH}

.PHONY: unit-test
unit-test:
	@echo
	@echo "==> Running unit tests <=="
	$(GO) test $(GOFLAGS) $(TESTS) $(TESTFLAGS)
