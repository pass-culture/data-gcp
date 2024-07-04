#!/bin/bash

# Write the output of the steps to a temporary file
echo "$1" > /tmp/output.json
echo "$2" >> /tmp/output.json
echo "$3" >> /tmp/output.json

# Display the contents of the temporary file
cat /tmp/output.json

# Compute the intersection of the JSON arrays
jobs_to_test=$(jq --compact-output --slurp '(.[0] + .[1]) - ((.[0] + .[1]) - .[2])' /tmp/output.json)

# Set the output variable
echo "jobs=${jobs_to_test}" | tee -a $GITHUB_OUTPUT
