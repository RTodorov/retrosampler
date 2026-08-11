#!/usr/bin/env bash
# L4: ADRs append-only; index updated in the same commit.
set -euo pipefail
entries=$(git diff --cached --name-status -- 'docs/adrs/*.md' | grep -v 'README.md' || true)
[[ -z "$entries" ]] && exit 0
need_index=0
while IFS=$'\t' read -r status f; do
  [[ -z "$status" ]] && continue
  case "$status" in
    D*)
      echo "BLOCKED: ADRs are append-only; $f cannot be deleted" >&2
      exit 1
      ;;
    A*) need_index=1 ;;
    M*)
      if git diff --cached -U0 -- "$f" | grep -E '^[+-][^+-]' | grep -vqE '^[+-]\s*-?\s*\*\*Status:\*\*'; then
        echo "BLOCKED: $f is merged; only its Status line may change. Supersede instead (docs/adrs/README.md)." >&2
        exit 1
      fi
      need_index=1
      ;;
  esac
done <<<"$entries"
if [[ $need_index -eq 1 ]] && ! git diff --cached --name-only | grep -q '^docs/adrs/README.md$'; then
  echo "BLOCKED: ADR change requires docs/adrs/README.md index update in the same commit" >&2
  exit 1
fi
