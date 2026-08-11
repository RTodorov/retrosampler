#!/usr/bin/env bash
# L1: lint only packages containing staged Go files.
set -euo pipefail
export PATH="$PWD/.tools:$PATH"
[[ $# -eq 0 ]] && exit 0
# internal/tools is a separate module (version pins only); outside root lint scope.
dirs=$(for f in "$@"; do [[ $f == *.go && $f != internal/tools/* ]] && dirname "$f"; done | sort -u || true)
[[ -z "$dirs" ]] && exit 0
targets=$(echo "$dirs" | sed 's|^|./|;s|$|/...|' | tr '\n' ' ')
# shellcheck disable=SC2086
golangci-lint run $targets
