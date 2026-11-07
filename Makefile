PREFIX ?= $(CURDIR)/target/install
ETCQLITE_BUILD_SHA := $(shell git rev-parse HEAD)
export ETCQLITE_BUILD_SHA
ETCQLITE_BUILD_DIRTY := $(if $(shell git status --porcelain --untracked-files=normal),true,false)
export ETCQLITE_BUILD_DIRTY

.PHONY: build install test fmt lint audit conformance

build:
	cargo build --release --locked
	go -C tools build -o ../target/release/etcqlite-snapshot ./snapshot

install:
	cargo install --path . --locked --root "$(PREFIX)"
	mkdir -p "$(PREFIX)/bin"
	go -C tools build -o "$(PREFIX)/bin/etcqlite-snapshot" ./snapshot

test:
	cargo test --locked
	go -C tools test ./...

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
