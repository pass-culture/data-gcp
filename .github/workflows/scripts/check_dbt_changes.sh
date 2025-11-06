#!/bin/bash

TARGET_BRANCH=$1

# Fetch the target branch
git fetch origin $TARGET_BRANCH

# Check for changes in the specified directory
changed_files=$(git diff origin/$TARGET_BRANCH HEAD --name-only | grep 'orchestration/dags/data_gcp_dbt/' || true)

# Set the output variable
if [[ -z "$changed_files" ]]; then
  echo "changes=false" >> $GITHUB_OUTPUT
else
  echo "changes=true" >> $GITHUB_OUTPUT
fi
