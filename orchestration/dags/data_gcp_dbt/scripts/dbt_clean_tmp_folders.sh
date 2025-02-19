#!/bin/bash
set -e
SCRIPT_DIR=$(dirname "$0")
source "$SCRIPT_DIR/helpers.sh"

# Step 1: Create the temporary folder
clean_old_folders "$PATH_TO_DBT_TARGET"


# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "fail cleaning folders. Exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
