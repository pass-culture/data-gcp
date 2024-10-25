#!/bin/bash

set -e

dbt deps --target $target

# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
