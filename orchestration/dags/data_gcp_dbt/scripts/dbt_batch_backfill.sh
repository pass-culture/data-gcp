#!/bin/bash

# Default values
START_DATE=${1:-"2025-03-01"}
END_DATE=${2:-"2025-01-01"}
STEP_DAYS=${3:-31}  # Default to 31 days if not specified
MODEL_NAME=${4:-"int_firebase__native_event"}
ENV_SHORT_NAME=${5:-"prod"}

# Function to run dbt for a specific date
run_backfill() {
    local date=$1
    echo "Running backfill for date: $date with lookback of $STEP_DAYS days"

    dbt run --select $MODEL_NAME \
        --vars "{\"lookback_days\": $STEP_DAYS, \"execution_date\": \"$date\", \"ENV_SHORT_NAME\": \"$ENV_SHORT_NAME\"}" \
        --target $ENV_SHORT_NAME
}

# Convert dates to timestamps for comparison
start_ts=$(date -j -f "%Y-%m-%d" "$START_DATE" "+%s")
end_ts=$(date -j -f "%Y-%m-%d" "$END_DATE" "+%s")

# Current date starts at start_date
current_ts=$start_ts

# Process each step
while [ $current_ts -ge $end_ts ]; do
    # Convert current timestamp to date
    current_date=$(date -j -f "%s" "$current_ts" "+%Y-%m-%d")

    # Run the backfill for this date
    run_backfill "$current_date"

    # Move back by step days
    current_ts=$(date -j -v-${STEP_DAYS}d -f "%s" "$current_ts" "+%s")
done

echo "Backfill completed!"
echo "Processed from $START_DATE to $END_DATE with step size of $STEP_DAYS days"
