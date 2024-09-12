#!/bin/bash

dbt snapshot --target $target --select $snapshot --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $PATH_TO_DBT_TARGET $GLOBAL_CLI_FLAGS
