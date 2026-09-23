GO ?= go
GO_MODULES := $(patsubst %/go.mod,%,$(shell find golang -type d -name vendor -prune -o -type f -name go.mod -print))

ifeq ($(strip $(GO_MODULES)),)
$(error No Go modules found)
endif

.PHONY: check fmt vet test build

check: fmt vet test build

# Like CI, fail if go fmt changes any file.
fmt:
	@set -eu; for module in $(GO_MODULES); do \
		printf 'fmt: %s\n' "$$module"; \
		formatted="$$( $(GO) -C "$$module" fmt ./... )"; \
		if [ -n "$$formatted" ]; then \
			printf 'Commit formatting changes in %s:\n%s\n' "$$module" "$$formatted" >&2; \
			exit 1; \
		fi; \
	done

vet test build:
	@set -eu; for module in $(GO_MODULES); do \
		printf '%s: %s\n' "$@" "$$module"; \
		$(GO) -C "$$module" $@ ./...; \
	done

# Run only after the coordinated tags described in golang/storage/RELEASING.md
# exist on origin. Unlike workspace checks, this must fail for unpublished versions.
RELEASE_VERSION ?= v0.1.0
STORAGE_MODULE := github.com/Analytical-Tradecraft-Technologies/cloud-storage/golang/storage

.PHONY: check-release
check-release:
	@set -eu; \
	consumer_dir="$$(mktemp -d)"; \
	trap 'rm -rf "$$consumer_dir"' EXIT HUP INT TERM; \
	cd "$$consumer_dir"; \
	export GOWORK=off GOFLAGS=; \
	$(GO) mod init cloud-storage-release-consumer; \
	$(GO) get "$(STORAGE_MODULE)/providercontracts@$(RELEASE_VERSION)" \
		"$(STORAGE_MODULE)/providers/aws@$(RELEASE_VERSION)" \
		"$(STORAGE_MODULE)/providers@$(RELEASE_VERSION)"; \
	printf '%s\n' 'package main' 'import (' \
		'"$(STORAGE_MODULE)/providercontracts/kv"' \
		'"$(STORAGE_MODULE)/providers"' \
		'awsprovider "$(STORAGE_MODULE)/providers/aws"' ')' \
		'func main() {' \
		' _ = kv.KeyValueStore.QueryPartition' \
		' _ = providers.FromJSON' \
		' _ = awsprovider.New' '}' > main.go; \
	$(GO) mod tidy; \
	test -z "$$($(GO) list -m -f '{{if .Replace}}{{.Path}}{{end}}' all)"; \
	$(GO) test ./...; \
	$(GO) build ./...
