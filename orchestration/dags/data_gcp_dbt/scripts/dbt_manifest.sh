#!/bin/bash

dbt ls --target $target --target-path $PATH_TO_DBT_TARGET --exclude fqn:* source:* exposure:* --select data_gcp_dbt --output json
