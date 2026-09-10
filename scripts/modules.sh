#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
find "$repo_root" -name go.mod -not -path "$repo_root/.git/*" -print \
  | sed "s|/go.mod$||" \
  | sort
