#!/bin/bash

# Step 1: Loop to generate a new random 12-character string for RDID until a non-existing folder is found
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

# Step 2: Create a temporary folder in $PATH_TO_DBT_TARGET/RDID using mkdir
TMP_FOLDER="$PATH_TO_DBT_TARGET/$RDID"
mkdir -p "$TMP_FOLDER"

# Step 3: Copy all files (but not folders) from $PATH_TO_DBT_TARGET to $TMP_FOLDER using cp
find "$PATH_TO_DBT_TARGET" -maxdepth 1 -type f -exec cp {} "$TMP_FOLDER/" \;

# Step 4: Run dbt with --target-path flag pointing to $TMP_FOLDER
dbt run --target $target --select $model $full_ref_str --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path "$TMP_FOLDER" $EXCLUSION $GLOBAL_CLI_FLAGS

# Step 5: Delete the temporary folder after dbt run completes
rm -rf "$TMP_FOLDER"
