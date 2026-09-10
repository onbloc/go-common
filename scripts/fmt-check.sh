#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if ! command -v golangci-lint >/dev/null 2>&1; then
  echo "golangci-lint is required. See CONTRIBUTING.md for the pinned version."
  exit 1
fi

while IFS= read -r module_dir; do
  echo "Checking formatting in ${module_dir#"$repo_root"/}..."
  (cd "$module_dir" && golangci-lint fmt --diff --config "$repo_root/.github/golangci.yaml")
done < <("$repo_root/scripts/modules.sh")
