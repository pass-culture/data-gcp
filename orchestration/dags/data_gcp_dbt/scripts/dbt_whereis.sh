#!/usr/bin/env bash

if [ -z "$1" ]; then
    echo "Usage: dbt-whereis <model_name>"
    exit 1
fi

MODEL_NAME="$1"

# Activate virtual environment if not already active (optional)
if [ -f "$(pwd)/.venv/bin/activate" ]; then
    source "$(pwd)/.venv/bin/activate"
fi

dbt run-operation where_is \
    --args "{'model_name':'$MODEL_NAME'}" \
    --target prod \
    --vars '{"ENV_SHORT_NAME":"prod"}'
