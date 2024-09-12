#!/bin/bash

edr send-report --project-dir . --target-path target/ --profiles-dir . --profile-target $ENV_SHORT_NAME --gcs-bucket-name $DATA_BUCKET_NAME --bucket-file-path $REPORT_FILE_PATH
