#!/bin/bash
set -xe


# Run the report
edr send-report \
    --project-dir "$PATH_TO_DBT_PROJECT" \
    --target-path "$DBT_TARGET_DIR" \
    --profiles-dir "$PATH_TO_DBT_PROJECT" \
    --profile-target "$ENV_SHORT_NAME" \
    --gcs-bucket-name "$DATA_BUCKET_NAME" \
    --bucket-file-path "$REPORT_FILE_PATH" \
    --slack-token "$SLACK_TOKEN" \
    --slack-channel-name "$CHANNEL_NAME" \
    --env "$ENV_SHORT_NAME" \
    $GLOBAL_CLI_FLAGS \
    --disable html_attachment \
    --days-back 1

# Capture exit code before cleanup
exit_code=$?

# Cleanup
rm -rf "$DBT_TARGET_DIR" "$DBT_PACKAGES_DIR"
rm -f "$PATH_TO_DBT_PROJECT/dbt_packages"

# Final exit
if [ $exit_code -ne 0 ]; then
    echo "dbt command failed with exit code $exit_code."
    exit $exit_code
fi
