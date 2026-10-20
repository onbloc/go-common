#!/usr/bin/env bash
# Fails unless the latest ci.yml push run on $GITHUB_SHA succeeded.
# Requires GH_TOKEN, GITHUB_REPOSITORY and GITHUB_SHA.
set -euo pipefail

run="$(gh api "repos/${GITHUB_REPOSITORY}/actions/workflows/ci.yml/runs?head_sha=${GITHUB_SHA}&event=push&per_page=100" \
  --jq '.workflow_runs | max_by(.run_started_at) // empty | [.html_url, .status, (.conclusion // "")] | @tsv')"

if [[ -z "$run" ]]; then
  echo "::error::No CI run found for ${GITHUB_SHA:0:7}; wait for CI to start" >&2
  exit 1
fi

IFS=$'\t' read -r url status conclusion <<< "$run"
if [[ "$status" != "completed" || "$conclusion" != "success" ]]; then
  echo "::error::CI is not green for ${GITHUB_SHA:0:7} (${status} ${conclusion}): ${url}" >&2
  exit 1
fi

echo "CI passed for ${GITHUB_SHA:0:7}: ${url}"
