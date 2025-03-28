#!/bin/bash

# Initialize flag for escaping
no_escape=false

# Parse arguments
for arg in "$@"; do
  if [ "$arg" == "--no-escape" ]; then
    no_escape=true
  fi
done

# Define a list of folders to ignore
ignored_folders=("jobs/ml_jobs/_template")

# Build the find command
find_cmd="find jobs -type d"
for ignored in "${ignored_folders[@]}"; do
  find_cmd="$find_cmd -not \( -path \"$ignored\" -prune \)"
done
find_cmd="$find_cmd -name tests -exec dirname {} \;"

# Execute the command
echo Find command: $find_cmd

# Initialize an array to hold the folder names
eval "$find_cmd" > /tmp/list.txt
folders=()
# Read the list of directories from the file and add them to the array
while IFS= read -r folder; do
  if [ "$no_escape" = true ]; then
    folders+=("'${folder}'")
  else
    folders+=(\\\"${folder}\\\")
  fi
done < /tmp/list.txt

# Create a JSON array from the folder names
json_array="[$(IFS=,; echo "${folders[*]}")]"

# Set the output variable
echo "testable_jobs=$json_array" | tee -a $GITHUB_OUTPUT
