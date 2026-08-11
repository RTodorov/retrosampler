#!/usr/bin/env bash
# L1: lint only packages containing staged Go files.
set -euo pipefail
export PATH="$PWD/.tools:$PATH"
[[ $# -eq 0 ]] && exit 0
dirs=$(for f in "$@"; do [[ $f == *.go ]] && dirname "$f"; done | sort -u || true)
[[ -z "$dirs" ]] && exit 0
targets=$(echo "$dirs" | sed 's|^|./|;s|$|/...|' | tr '\n' ' ')
# shellcheck disable=SC2086
golangci-lint run $targets
