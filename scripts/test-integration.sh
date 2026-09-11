#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

while IFS= read -r module_dir; do
  echo "Running integration tests in ${module_dir#"$repo_root"/}..."
  (cd "$module_dir" && go test -v ./... -timeout 10m)
done < <(find "$repo_root" -type d -path "*/tests/integration" -exec test -f "{}/go.mod" \; -print | sort)
