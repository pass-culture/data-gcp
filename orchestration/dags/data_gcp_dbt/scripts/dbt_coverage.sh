#!/bin/bash

# Default values
JSON_FILE="./target/coverage.json"
COVERAGE_THRESHOLD=1
TARGET="prod"
TARGET_PATH="./target"  # default path for target files

# Check for optional arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --target) TARGET="$2"; shift ;;
        --coverage) COVERAGE_THRESHOLD="$2"; shift ;;
        --target-path) TARGET_PATH="$2"; JSON_FILE="$TARGET_PATH/coverage.json"; shift ;;
    esac
    shift
done

dbt docs generate --target="$TARGET"  --no-compile --threads=8

dbt-coverage compute doc --model-path-filter models/mart/ --cov-report "$JSON_FILE"

python scripts/coverage.py "$JSON_FILE" "$COVERAGE_THRESHOLD"
