#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fingerprint() {
  for file in go.mod go.sum; do
    if [[ -f "$file" ]]; then
      git hash-object "$file"
    else
      echo missing
    fi
  done
}

while IFS= read -r module_dir; do
  echo "Checking module files in ${module_dir#"$repo_root"/}..."
  before="$(cd "$module_dir" && fingerprint)"
  (cd "$module_dir" && go mod tidy)
  after="$(cd "$module_dir" && fingerprint)"

  if [[ "$before" != "$after" ]]; then
    echo "go mod tidy changed module files. Run 'make tidy' and commit the result."
    exit 1
  fi
done < <("$repo_root/scripts/modules.sh")
