#!/bin/bash

find_dotenv() {
  local dir="$PWD"
  while [[ "$dir" != "/" ]]; do
    if [[ -f "$dir/.env.buckets" ]]; then
      echo "$dir/.env.buckets"
      return 0
    fi
    dir=$(dirname "$dir")
  done
  return 1  # .env not found
}

# Find and load the .env file
DOTENV_FILE=$(find_dotenv)
if [[ -n "$DOTENV_FILE" ]]; then
  # Load only valid environment variables from the .env file, ignoring comments and invalid lines
  export $(grep -v '^#' "$DOTENV_FILE" | xargs)
  echo "Loaded environment variables from $DOTENV_FILE"
else
  echo "Error: .env.buckets file not found in current or parent directories."
  exit 1
fi

# Define the dbt alias or function
dbt() {
  # Check if the first argument is one of 'run' or 'test'
  if [[ "$1" == "run" || "$1" == "test" ]]; then
    echo "Invoking dbt_hook for $1 command."
    # Call dbt_hook and pass all the original arguments
    dbt_hook "$@"
  elif [[ "$1" == --sync-artifacts=* ]]; then
    echo "Invoking dbt_sync_artifacts for sync-artifacts."
    # Extract the environment from the argument
    DEFER_LOCAL_RUN_TO="${1#*=}"
    # Call dbt_sync_artifacts with the extracted environment
    dbt_sync_artifacts "$DEFER_LOCAL_RUN_TO"
  elif [[ "$1" == "--sync-artifacts" ]]; then
    echo "Invoking dbt_sync_artifacts for all environments."
    # Call dbt_sync_artifacts for each environment
    for env in dev stg prod; do
      dbt_sync_artifacts "$env"
    done
  else
    echo "Skipping dbt_hook for $1 command."
    command dbt "$@"
  fi
}

## dbt sync artifacts function
dbt_sync_artifacts() {
  local DEFER_LOCAL_RUN_TO="$1"

  # Determine the GCS bucket based on DEFER_LOCAL_RUN_TO
  case "$DEFER_LOCAL_RUN_TO" in
    dev)
      GCS_BUCKET_PATH="gs://${COMPOSER_BUCKET_DEV}/data/target"
      ;;
    stg)
      GCS_BUCKET_PATH="gs://${COMPOSER_BUCKET_STG}/data/target"
      ;;
    prod)
      GCS_BUCKET_PATH="gs://${COMPOSER_BUCKET_PROD}/data/target"
      ;;
    *)
      echo "Unknown environment: $DEFER_LOCAL_RUN_TO. Skipping artifact pulling."
      return 1
      ;;
  esac

  # Ensure target directory exists
  TARGET_DIR="target"
  if [ ! -d "$TARGET_DIR" ]; then
    echo "Target directory does not exist. Creating $TARGET_DIR"
    mkdir -p "$TARGET_DIR"
  fi

  # Ensure the environment-specific artifacts directory exists
  ARTIFACTS_DIR="${TARGET_DIR}/${DEFER_LOCAL_RUN_TO}-run-artifacts"
  if [ ! -d "$ARTIFACTS_DIR" ]; then
    echo "${DEFER_LOCAL_RUN_TO}-run-artifacts directory does not exist. Creating $ARTIFACTS_DIR"
    mkdir -m 777 "$ARTIFACTS_DIR"
  else
    echo "${DEFER_LOCAL_RUN_TO}-run-artifacts directory already exists."
  fi

  # Pull only files from the remote GCS bucket
  echo "Pulling files from $GCS_BUCKET_PATH to $ARTIFACTS_DIR"
  gsutil cp "$GCS_BUCKET_PATH/manifest.json" "$ARTIFACTS_DIR/manifest.json"
  gsutil cp "$GCS_BUCKET_PATH/run_results.json" "$ARTIFACTS_DIR/run_results.json"

  # Check if the gsutil command succeeded
  if [ $? -eq 0 ]; then
    echo "Artifacts pulled successfully for $DEFER_LOCAL_RUN_TO."
  else
    echo "Failed to pull artifacts from $GCS_BUCKET_PATH for $DEFER_LOCAL_RUN_TO."
    return 1
  fi
}

## dbt deferral hook
dbt_hook() {
  # Set default values for flags
  DEFER_LOCAL_RUN_TO="dev"
  REFRESH_STATE=false
  SYNC_ARTIFACTS=false
  SYNC_ENVIRONMENTS=("dev" "stg" "prod")

  # Parse arguments and find flags if present
  for arg in "$@"; do
    case "$arg" in
      --defer-to=*)
        DEFER_LOCAL_RUN_TO="${arg#*=}"
        ;;
      --refresh-state)
        REFRESH_STATE=true
        ;;
      --sync-artifacts)
        SYNC_ARTIFACTS=true
        SYNC_ENVIRONMENTS=("dev" "stg" "prod")  # Default to all environments if no specific value is provided
        ;;
      --sync-artifacts=*)
        SYNC_ARTIFACTS=true
        SYNC_ARG="${arg#*=}"

        if [[ "$SYNC_ARG" == "dev" || "$SYNC_ARG" == "stg" || "$SYNC_ARG" == "prod" ]]; then
          SYNC_ENVIRONMENTS=("$SYNC_ARG")  # Use the specific environment provided
        else
          echo "Error: Invalid environment specified with --sync-artifacts. Allowed values are 'all', 'dev', 'stg', 'prod'."
          return 1
        fi
        ;;
    esac
  done

  # Check if --sync-artifacts is used alongside other arguments
  if [[ "$SYNC_ARTIFACTS" == true ]]; then
    if [[ "$#" -gt 1 ]]; then
      echo "Error: The --sync-artifacts flag must be used alone. No other arguments are allowed."
      echo "Hint: If you want to refresh artifacts and run a dbt command, use --refresh-state instead."
      return 1  # Cancel the script
    fi
  fi

  # Set project directory (current directory by default)
  PROJECT_DIR=$(pwd)

  # Function to find dbt_project.yml file
  find_dbt_project_yml() {
    local dir="$1"
    while [[ "$dir" != "/" ]]; do
      if [[ -f "$dir/dbt_project.yml" ]]; then
        echo "$dir"
        return 0
      fi
      dir=$(dirname "$dir")
    done
    return 1
  }

  # Check if dbt_project.yml exists in the current or parent directories
  DBT_PROJECT_DIR=$(find_dbt_project_yml "$PROJECT_DIR")
  if [[ -z "$DBT_PROJECT_DIR" ]]; then
    echo "Error: dbt_project.yml not found. This script must be run from within a dbt project folder."
    return 1
  fi
  DBT_PROFILES_DIR=$DBT_PROJECT_DIR

  # Function to pull artifacts from a specific environment (manifest and run_results)
  pull_artifacts() {
    local env="$1"

    case "$env" in
      dev)
        GCS_BUCKET_PATH="gs://${COMPOSER_BUCKET_DEV}/data/target"
        ;;
      stg)
        GCS_BUCKET_PATH="gs://${COMPOSER_BUCKET_STG}/data/target"
        ;;
      prod)
        GCS_BUCKET_PATH="gs://${COMPOSER_BUCKET_PROD}/data/target"
        ;;
      *)
        echo "Unknown environment: $env. Unable to pull artifacts."
        return 1
        ;;
    esac

    # Ensure the directory exists
    mkdir -p "${DBT_PROJECT_DIR}/env-run-artifacts/${env}"

    # Pull the manifest.json
    gsutil cp "$GCS_BUCKET_PATH/manifest.json" "${DBT_PROJECT_DIR}/env-run-artifacts/${env}/manifest.json"
    if [ $? -ne 0 ]; then
      echo "Failed to pull manifest.json from $GCS_BUCKET_PATH."
      return 1
    fi

    # Pull the run_results.json
    gsutil cp "$GCS_BUCKET_PATH/run_results.json" "${DBT_PROJECT_DIR}/env-run-artifacts/${env}/run_results.json"
    if [ $? -ne 0 ]; then
      echo "Failed to pull run_results.json from $GCS_BUCKET_PATH."
      return 1
    fi

    echo "Artifacts (manifest.json and run_results.json) pulled successfully from $env."
  }

  # If --sync-artifacts is provided, pull artifacts and exit
  if [[ "$SYNC_ARTIFACTS" == true ]]; then
    echo "Syncing artifacts from environments: ${SYNC_ENVIRONMENTS[*]}"

    # Pull artifacts from all specified environments
    for env in "${SYNC_ENVIRONMENTS[@]}"; do
      pull_artifacts "$env"
    done

    # End script here as no dbt command is needed
    return 0
  fi

  # If --refresh-state is provided and no dbt command (e.g., run, test) is specified:
  if [[ "$REFRESH_STATE" == true && -z "$1" ]]; then
    echo "Refreshing state without dbt command: pulling from dev, stg, and prod."

    # Pull artifacts from all environments (dev, stg, prod)
    for env in dev stg prod; do
      pull_artifacts "$env"
    done

    # End script here as no dbt command is needed
    return 0
  fi

  # If --refresh-state and --defer-to are provided:
  if [[ "$REFRESH_STATE" == true && "$DEFER_LOCAL_RUN_TO" != "none" ]]; then
    echo "Refreshing state and pulling artifacts (manifest.json and run_results.json) from $DEFER_LOCAL_RUN_TO."

    # Pull both manifest.json and run_results.json from the specified environment
    pull_artifacts "$DEFER_LOCAL_RUN_TO"

    # End script here as no dbt command is needed
    return 0
  fi

  # The rest of the dbt hook logic for standard dbt run/test
  ARTIFACTS_DIR="${DBT_PROJECT_DIR}/env-run-artifacts/${DEFER_LOCAL_RUN_TO}"
  DEFER_FLAGS=()  # Initialize defer flags
  if [[ "$DEFER_LOCAL_RUN_TO" != "none" ]]; then
    MANIFEST_PATH="$ARTIFACTS_DIR/manifest.json"
    RESULTS_PATH="$ARTIFACTS_DIR/run_results.json"

    # Determine if artifacts need to be pulled
    NEED_PULL=false
    if [ ! -f "$MANIFEST_PATH" ] || [ ! -f "$RESULTS_PATH" ] || [[ "$REFRESH_STATE" == true ]]; then
      NEED_PULL=true
      echo "Pulling or refreshing artifacts..."
    fi

    if [[ "$NEED_PULL" == true ]]; then
      pull_artifacts "$DEFER_LOCAL_RUN_TO"
      if [ $? -ne 0 ]; then
        return 1
      fi
    else
      echo "Using existing artifacts. Consider using --refresh-state to ensure freshness."
    fi

    # Set defer flags to be passed to the dbt command
    DEFER_FLAGS=(--defer --state "$ARTIFACTS_DIR" --favor-state)
  else
    echo "Skipping artifact pulling as --defer-to=none."
  fi

  # Remove specific arguments before passing the rest to dbt
  local FILTERED_ARGS=()
  for arg in "$@"; do
    if [[ "$arg" != --defer-to=* && "$arg" != --refresh-state ]]; then
      FILTERED_ARGS+=("$arg")
    fi
  done

  # Combine filtered arguments with defer flags
  local COMBINED_ARGS=("${FILTERED_ARGS[@]}" "${DEFER_FLAGS[@]}")

  echo "Running dbt with arguments: ${COMBINED_ARGS[@]}"
  export DBT_PROFILES_DIR=$DBT_PROJECT_DIR
  command dbt "${COMBINED_ARGS[@]}"
}

# Call the dbt function with the provided arguments
dbt "$@"
