#!/bin/bash

set -ex

dbt test ${SELECT:+--select "$SELECT"} \
        --target "$target" \
        --target-path "$PATH_TO_DBT_TARGET" \
        $GLOBAL_CLI_FLAGS \
        --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" \
        --exclude $EXCLUSION

# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
