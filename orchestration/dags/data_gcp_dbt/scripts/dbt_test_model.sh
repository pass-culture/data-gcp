#!/bin/bash

dbt $GLOBAL_CLI_FLAGS test --target $target --select $model --target-path $PATH_TO_DBT_TARGET --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}"
