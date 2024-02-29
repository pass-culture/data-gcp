#!/bin/bash

dbt test --target $target --target-path $PATH_TO_DBT_TARGET $GLOBAL_CLI_FLAGS --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --exclude $EXCLUSION

