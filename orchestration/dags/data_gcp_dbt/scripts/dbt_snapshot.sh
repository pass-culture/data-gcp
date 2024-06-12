#!/bin/bash

dbt $GLOBAL_CLI_FLAGS snapshot --target $target --select $snapshot --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $PATH_TO_DBT_TARGET 
