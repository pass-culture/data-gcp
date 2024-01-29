#!/bin/bash

dbt ls --target $target --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $PATH_TO_DBT_TARGET --exclude fqn:* source:* exposure:* --select data_gcp_dbt --output json
