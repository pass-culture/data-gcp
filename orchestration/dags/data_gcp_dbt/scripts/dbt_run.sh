#!/bin/bash

dbt run --target $target --select $model $full_ref_str --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $PATH_TO_DBT_TARGET $GLOBAL_CLI_FLAGS
