#!/bin/bash

dbt run --models package:$package_name --target $target $full_ref_str --target-path $PATH_TO_DBT_TARGET $GLOBAL_CLI_FLAGS

