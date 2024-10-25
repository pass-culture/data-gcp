#!/bin/bash

set -e

dbt ls --target $target --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $PATH_TO_DBT_TARGET --exclude fqn:* source:* exposure:* --select data_gcp_dbt --output json

# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
