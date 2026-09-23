#!/usr/bin/env bash
set -euo pipefail

# Keep one stable CI check while validating every independent Go module.
cd "$(dirname "$0")/.." || exit 1
command_name="${1:-}"
case "$command_name" in
  fmt|vet|test|build) ;;
  *) echo 'Usage: bash scripts/check-go.sh {fmt|vet|test|build}' >&2; exit 2 ;;
esac

found=false
while IFS= read -r -d '' manifest; do
  found=true
  module_directory="$(dirname "$manifest")"
  printf '%s: %s\n' "$command_name" "$module_directory"
  if [[ "$command_name" == fmt ]]; then
    formatted="$(go -C "$module_directory" fmt ./...)"
    if [[ -n "$formatted" ]]; then
      printf 'Run go fmt ./... in %s and commit the changes to:\n%s\n' "$module_directory" "$formatted" >&2
      exit 1
    fi
  else
    go -C "$module_directory" "$command_name" ./...
  fi
done < <(find golang -type d -name vendor -prune -o -type f -name go.mod -print0)

if [[ "$found" == false ]]; then
  echo 'No Go modules found.' >&2
  exit 1
fi
