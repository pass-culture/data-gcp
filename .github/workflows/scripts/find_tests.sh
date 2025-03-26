#!/bin/bash

# Define a list of folders to ignore
ignored_folders=("_template")

# Build the find exclusion patterns
find_exclusions=""
for ignored in "${ignored_folders[@]}"; do
  find_exclusions="$find_exclusions -not -path \"*$ignored*\""
done

# Find directories named "tests" and get their parent directories, excluding ignored folders
find jobs -type d -name "tests" $find_exclusions -exec dirname {} \; > /tmp/list.txt

# Initialize an array to hold the folder names
folders=()

# Read the list of directories from the file and add them to the array
while IFS= read -r folder; do
  folders+=(\\\"${folder}\\\")
done < /tmp/list.txt

# Create a JSON array from the folder names
json_array="[$(IFS=,; echo "${folders[*]}")]"

# Set the output variable
echo "testable_jobs=$json_array" | tee -a $GITHUB_OUTPUT
