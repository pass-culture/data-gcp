#!/bin/bash

set -euo pipefail
# Check if TARGET_BRANCH is provided
if [ -z "$1" ]; then
  echo "TARGET_BRANCH is required as the first argument"
  exit 1
fi
if [ -z "$2" ]; then
  echo "WORKING_DIR is required as the second argument"
  exit 1
fi


TARGET_BRANCH=$1
WORKING_DIR=$2
children="+1"
cd $WORKING_DIR


dbt_run_changed_models() {
  git fetch origin $TARGET_BRANCH
  models=$(git diff origin/$TARGET_BRANCH HEAD --name-only | grep 'orchestration/dags/data_gcp_dbt/models/' | grep '\.sql$' | awk -F '/' '{ print $NF }' | sed "s/\.sql$/${children}/g" | tr '\n' ' ')

  echo ${#models}
  if [ -z "$models" ]; then
    echo "no models were modified"
    echo "should_run_tests=false" >> "$GITHUB_OUTPUT"
  else
    echo "Running models: ${models}"
    dbt run --model $models --profile CI --target $ENV_SHORT_NAME --defer --state env-run-artifacts --favor-state --vars "{'CI_MATERIALIZATION':'view','ENV_SHORT_NAME':'$ENV_SHORT_NAME'}" --exclude tag:failing_ci tag:exclude_ci
    echo "should_run_tests=true" >> "$GITHUB_OUTPUT"
  fi

}

dbt_run_changed_models
