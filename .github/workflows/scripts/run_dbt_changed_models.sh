#!/usr/bin/env bash
set -Eeuo pipefail

# Args
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <TARGET_BRANCH> <WORKING_DIR>"
  exit 1
fi

TARGET_BRANCH="$1"
WORKING_DIR="$2"
children="+1"

cd "$WORKING_DIR"

dbt_run_changed_models() {
  git fetch origin "$TARGET_BRANCH"

  # Collect changed SQL models under models/
  mapfile -t changed_sql < <(
    git diff "origin/$TARGET_BRANCH"...HEAD --name-only \
      | grep -E '^orchestration/dags/data_gcp_dbt/models/.*\.sql$' || true
  )

  if (( ${#changed_sql[@]} == 0 )); then
    echo "no models were modified"
    echo "should_run_tests=false" >> "$GITHUB_OUTPUT"
    return 0
  fi

  # Turn file names into dbt selectors with +1 children
  models=$(printf '%s\n' "${changed_sql[@]}" \
    | awk -F '/' '{ print $NF }' \
    | sed "s/\.sql$/${children}/" \
    | tr '\n' ' ')

  echo "Running models: ${models}"

  # Run dbt and capture exit status
  set +e
  dbt run \
    --model $models \
    --profile CI \
    --target "$ENV_SHORT_NAME" \
    --defer \
    --state env-run-artifacts \
    --favor-state \
    --vars "{'CI_MATERIALIZATION':'view','ENV_SHORT_NAME':'$ENV_SHORT_NAME'}" \
    --exclude tag:failing_ci tag:exclude_ci
  dbt_status=$?
  set -e

  if [[ $dbt_status -ne 0 ]]; then
    echo "dbt run failed with status $dbt_status"
    echo "should_run_tests=false" >> "$GITHUB_OUTPUT"
    exit $dbt_status
  fi

  echo "should_run_tests=true" >> "$GITHUB_OUTPUT"
}

dbt_run_changed_models
