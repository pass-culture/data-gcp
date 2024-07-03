#!/bin/bash

# Check if TARGET_BRANCH is provided
if [ -z "$1" ]; then
  echo "TARGET_BRANCH is required as the first argument"
  exit 1
fi

TARGET_BRANCH=$1
children="+1"
WORKING_DIR="orchestration/dags/data_gcp_dbt/"
cd $WORKING_DIR || exit


dbt_run_changed_models() {
  git fetch origin $TARGET_BRANCH
  models=$(git diff origin/$TARGET_BRANCH HEAD --name-only | grep 'orchestration/dags/data_gcp_dbt/models/' | grep '\.sql$' | awk -F '/' '{ print $NF }' | sed "s/\.sql$/${children}/g" | tr '\n' ' ')
  echo ${#models}
  if [ -z "$models" ]; then
    echo "no models were modified"
  else
    echo "Running models: ${models}"
    dbt run --model $models --exclude config.materialized:incremental --profile CI --target $ENV_SHORT_NAME --defer --state env-run-artifacts --favor-state --vars "{'CI_MATERIALIZATION':'view','ENV_SHORT_NAME':'$ENV_SHORT_NAME'}"
  fi
}

dbt_run_changed_models
