#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
while IFS= read -r module_dir; do
  echo "Vetting ${module_dir#"$repo_root"/}..."
  (cd "$module_dir" && go vet ./...)
done < <("$repo_root/scripts/modules.sh")
