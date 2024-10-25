#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Step 1: Set the trap to clean up on script exit
trap 'cleanup' EXIT

# Cleanup function to remove temporary folder
cleanup() {
  if [ -d "$TMP_FOLDER" ]; then
    echo "Cleaning up... Deleting temporary folder: $TMP_FOLDER"
    rm -rf "$TMP_FOLDER"
  fi
}

# Step 2: Loop to generate a new random 12-character string for RDID until a non-existing folder is found
while true; do
  # Generate a random 12-character string for RDID
  RDID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
  TMP_FOLDER="$PATH_TO_DBT_TARGET/tmp_$RDID"

  # Check if the folder exists
  if [ -d "$TMP_FOLDER" ]; then
    echo "Temporary folder $TMP_FOLDER already exists. Generating a new RDID..."
  else
    # If the folder doesn't exist, break the loop
    mkdir -p "$TMP_FOLDER"
    echo "Created temporary folder: $TMP_FOLDER"
    break
  fi
done

# Step 3: Copy all files (but not folders) from $PATH_TO_DBT_TARGET to $TMP_FOLDER using cp
find "$PATH_TO_DBT_TARGET" -maxdepth 1 -type f -exec cp {} "$TMP_FOLDER/" \;

# Step 4: Run dbt with --target-path flag pointing to $TMP_FOLDER
dbt test --target $target --select $model --target-path $TMP_FOLDER --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" $GLOBAL_CLI_FLAGS

# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
