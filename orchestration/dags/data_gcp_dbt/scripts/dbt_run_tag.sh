#!/bin/bash
set -e
SCRIPT_DIR=$(dirname "$0")
source "$SCRIPT_DIR/helpers.sh"

# Step 1: Create the temporary folder
create_tmp_folder "$PATH_TO_DBT_TARGET"

# Step 2: Copy all files
copy_files_to_tmp_folder "$PATH_TO_DBT_TARGET"

# Step 4: Run dbt with --target-path flag pointing to $TMP_FOLDER
dbt run --target $target --select tag:$tag $full_ref_str --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $TMP_FOLDER $EXCLUSION $GLOBAL_CLI_FLAGS

# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
