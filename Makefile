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
