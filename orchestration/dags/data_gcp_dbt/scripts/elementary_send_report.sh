#!/bin/bash

sudo chmod -R 777 $ELEMENTARY_PYTHON_PATH
edr send-report --project-dir $PATH_TO_DBT_PROJECT --target-path $PATH_TO_DBT_TARGET --profiles-dir $PATH_TO_DBT_PROJECT --profile-target $ENV_SHORT_NAME --gcs-bucket-name $DATA_BUCKET_NAME --bucket-file-path $REPORT_FILE_PATH --slack-token $SLACK_TOKEN --slack-channel-name $CHANNEL_NAME
