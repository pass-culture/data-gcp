#!/bin/bash

set -e

sudo chmod -R 777 $ELEMENTARY_PYTHON_PATH | true
edr send-report --project-dir $PATH_TO_DBT_PROJECT --target-path $PATH_TO_DBT_TARGET --profiles-dir $PATH_TO_DBT_PROJECT --profile-target $ENV_SHORT_NAME --gcs-bucket-name $DATA_BUCKET_NAME --bucket-file-path $REPORT_FILE_PATH --slack-token $SLACK_TOKEN --slack-channel-name $CHANNEL_NAME --env $ENV_SHORT_NAME $GLOBAL_CLI_FLAGS --disable html_attachment --days-back $LOOKBACK

# Capture the exit code and propagate it
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code  # Ensure the script exits with dbt's exit code
fi
