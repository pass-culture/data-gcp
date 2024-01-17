#!/bin/bash

dbt test --target $target --select $model $full_ref_str --target-path $PATH_TO_DBT_TARGET $GLOBAL_CLI_FLAGS
