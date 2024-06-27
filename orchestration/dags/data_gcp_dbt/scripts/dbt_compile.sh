#!/bin/bash

dbt compile --target $target --vars "{ENV_SHORT_NAME: $ENV_SHORT_NAME}" --target-path $PATH_TO_DBT_TARGET
