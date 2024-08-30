#!/bin/bash

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
cd $WORKING_DIR

models=$(git diff origin/$TARGET_BRANCH HEAD --name-only | grep 'orchestration/dags/data_gcp_dbt/models/' | grep '\.sql$')

dbt_lint_changed_models() {
  git fetch origin $TARGET_BRANCH
  models=$(git diff origin/$TARGET_BRANCH HEAD --name-only | grep 'orchestration/dags/data_gcp_dbt/models/' | grep '\.sql$' | tr '\n' ' ')
  echo ${#models}
  if [ -z "$models" ]; then
    echo "no models were modified"
  else
    echo "Linting models: ${models}"
    make sqlfluff_lint_ci $models
  fi
}

dbt_lint_changed_models()
