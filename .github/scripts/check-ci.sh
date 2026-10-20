#!/usr/bin/env bash
# Fails unless every check run on $GITHUB_SHA (except the current workflow run) succeeded.
# Requires GH_TOKEN, GITHUB_REPOSITORY, GITHUB_SHA and GITHUB_RUN_ID.
set -euo pipefail

runs="$(gh api --paginate "repos/${GITHUB_REPOSITORY}/commits/${GITHUB_SHA}/check-runs?per_page=100" \
  --jq ".check_runs[] | select(.details_url | contains(\"/actions/runs/${GITHUB_RUN_ID}/\") | not) | [.name, .status, (.conclusion // \"\")] | @tsv")"

if [[ -z "$runs" ]]; then
  echo "::error::No CI check runs found for ${GITHUB_SHA:0:7}; wait for CI to start" >&2
  exit 1
fi

bad="$(awk -F'\t' '!($2 == "completed" && ($3 == "success" || $3 == "skipped" || $3 == "neutral")) { printf "  %s (%s %s)\n", $1, $2, $3 }' <<< "$runs")"
if [[ -n "$bad" ]]; then
  echo "::error::CI is not green for ${GITHUB_SHA:0:7}; wait for it to finish or fix failures:" >&2
  echo "$bad" >&2
  exit 1
fi

echo "CI passed for ${GITHUB_SHA:0:7}"
