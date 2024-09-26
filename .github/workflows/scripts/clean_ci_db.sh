#!/bin/bash

# Check if TARGET_BRANCH is provided
if [ -z "$1" ]; then
  echo "TARGET_BRANCH is required as the first argument"
  exit 1
fi

# Check if WORKING_DIR is provided
if [ -z "$2" ]; then
  echo "WORKING_DIR is required as the second argument"
  exit 1
fi

TARGET_BRANCH=$1
WORKING_DIR=$2
parents="1+"

# Change to the working directory
cd $WORKING_DIR || { echo "Failed to change directory to $WORKING_DIR"; exit 1; }

# Function to delete CI parent views
dbt_run_op_delete_ci_parents_views() {
  git fetch origin $TARGET_BRANCH

  # Get modified models
  models=$(git diff origin/$TARGET_BRANCH HEAD --name-only | grep 'orchestration/dags/data_gcp_dbt/models/' | grep '\.sql$' | awk -F '/' '{ print $NF }' | sed "s/\.sql$/${children}/g" | tr '\n' ' ')

  if [ -z "$models" ]; then
    echo "No models were modified"
  else
    echo "Cleaning CI views for ${models} ${parents} parents"
    # Generate the list of parent models
    model_list=$(dbt list -s "${parents}${models}" --exclude "${models}" --exclude-resource-type source test | grep data_gcp_dbt | awk -F '.' '{ print $NF }' | tr '\n' ',' | sed "s/,$//g")

    # Format the model list for JSON
    model_list="['$(echo $model_list | sed "s/,/','/g")']"

    echo "Parents list to delete: $model_list"

    # Run the dbt operation to delete the views
    dbt run-operation delete_views --args "{\"models\": $model_list}" --profile CI
  fi
}

# Run the function to delete CI parent views
dbt_run_op_delete_ci_parents_views
