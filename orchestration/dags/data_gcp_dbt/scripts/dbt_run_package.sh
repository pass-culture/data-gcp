#!/bin/bash

dbt run --models package:$package_name --target $target --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" $full_ref_str --target-path $PATH_TO_DBT_TARGET $GLOBAL_CLI_FLAGS

