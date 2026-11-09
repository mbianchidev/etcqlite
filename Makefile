PREFIX ?= $(CURDIR)/target/install
VERSION := $(shell sed -n 's/^version = "\([^"]*\)"$$/\1/p' Cargo.toml | head -n 1)
ETCQLITE_BUILD_SHA := $(shell git rev-parse HEAD)
export ETCQLITE_BUILD_SHA
ETCQLITE_BUILD_DIRTY := $(if $(shell git status --porcelain --untracked-files=normal),true,false)
export ETCQLITE_BUILD_DIRTY
GO_BUILD_FLAGS = -mod=readonly -trimpath -ldflags "-X main.version=$(VERSION) -X main.buildSHA=$(ETCQLITE_BUILD_SHA) -X main.buildDirty=$(ETCQLITE_BUILD_DIRTY)"

.PHONY: build install test test-campaign test-distribution fmt lint audit conformance

build:
	cargo build --release --locked
	go -C tools build $(GO_BUILD_FLAGS) -o ../target/release/etcqlite-snapshot ./snapshot

install:
	cargo install --path . --locked --root "$(PREFIX)"
	mkdir -p "$(PREFIX)/bin"
	go -C tools build $(GO_BUILD_FLAGS) -o "$(PREFIX)/bin/etcqlite-snapshot" ./snapshot

test:
	cargo test --locked
	go -C tools test ./...
	$(MAKE) test-campaign
	$(MAKE) test-distribution

test-campaign:
	PYTHONDONTWRITEBYTECODE=1 python3 scripts/test_qualification_campaign.py

test-distribution:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s script -p 'test_*.py'

fmt:
	cargo fmt --all --check
	test -z "$$(gofmt -l tools)"

lint:
	cargo clippy --locked --all-targets -- -D warnings
	go -C tools vet ./...

audit:
	cargo audit --deny warnings
	target/tools/govulncheck -C tools ./...

conformance:
	go -C tools test -count=1 -v ./conformance
