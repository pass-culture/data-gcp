#!/bin/bash

dbt $GLOBAL_CLI_FLAGS run --models package:$package_name --target $target $full_ref_str --target-path $PATH_TO_DBT_TARGET

