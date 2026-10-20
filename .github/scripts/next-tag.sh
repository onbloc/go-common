#!/usr/bin/env bash
# Usage: next-tag.sh <module> <patch|minor|major>
# Prints "tag=<module>/vX.Y.Z" and "prev=<previous tag or empty>" for the next release.
set -euo pipefail

module="${1:?module required}"
bump="${2:?bump required}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
gomod="$repo_root/$module/go.mod"
[[ -f "$gomod" ]] || { echo "::error::$module/go.mod not found" >&2; exit 1; }

# Required major from module path suffix (/vN, N>=2); otherwise v0 or v1.
path="$(awk '$1 == "module" { print $2; exit }' "$gomod")"
if [[ "$path" =~ /v([0-9]+)$ && "${BASH_REMATCH[1]}" -ge 2 ]]; then
  required="${BASH_REMATCH[1]}"
else
  required=""
fi

prev="$(git -C "$repo_root" tag --list "${module}/v*" --sort=-v:refname \
  | grep -E "^${module}/v[0-9]+\.[0-9]+\.[0-9]+$" | head -n1 || true)"

if [[ -z "$prev" ]]; then
  # First release: v0.1.0, or vN.0.0 for a /vN module path
  if [[ -n "$required" ]]; then major="$required"; minor=0; else major=0; minor=1; fi
  patch=0
else
  IFS=. read -r major minor patch <<< "${prev#"${module}"/v}"
  case "$bump" in
    major) major=$((major + 1)); minor=0; patch=0 ;;
    minor) minor=$((minor + 1)); patch=0 ;;
    patch) patch=$((patch + 1)) ;;
    *) echo "::error::bump must be patch, minor or major" >&2; exit 1 ;;
  esac
fi

if [[ -n "$required" && "$major" != "$required" ]] \
  || [[ -z "$required" && "$major" -ge 2 ]]; then
  echo "::error::v${major}.${minor}.${patch} does not match module path '${path}'; a new major requires a /v${major} module path suffix" >&2
  exit 1
fi

tag="${module}/v${major}.${minor}.${patch}"
if git -C "$repo_root" rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  echo "::error::Tag ${tag} already exists" >&2
  exit 1
fi

echo "tag=${tag}"
echo "prev=${prev}"
