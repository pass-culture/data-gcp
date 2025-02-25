#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

clean_old_folders() {
  local base_dir=$1  # Base directory where the temporary folder will be created (required)
  local folder_prefix=$2  # Optional folder prefix (default: "tmp")
  local days_old=$3  # Optional number of days to keep, default is 1 day

  folder_prefix=${folder_prefix:-tmp}
  days_old=${days_old:-1}

  # Validate required parameters
  if [ -z "$base_dir" ]; then
    echo "ERROR: Base directory is required for folder cleanup."
    exit 1
  fi

  # Ensure directory exists before attempting cleanup
  if [ ! -d "${base_dir}/${folder_prefix}" ]; then
    echo "WARNING: Directory '${base_dir}/${folder_prefix}' does not exist. Skipping cleanup."
    return 0
  fi



  echo "Cleaning up old temporary folders in '${base_dir}/${folder_prefix}'..."

  # Calculate the cutoff date (current date - days_old)
  cutoff_date=$(date -d "-${days_old} days" +"%Y%m%d")

  # Loop through directories matching "exec_*"
  find "${base_dir}/${folder_prefix}" -type d -name "exec_*" | while read -r folder; do
    # Extract the date part from the folder name (assumes format: exec_YYYYMMDD_HHMMSS_xxx)
    folder_date=$(basename "$folder" | awk -F'_' '{print $2}')

    # Check if folder_date is older than cutoff_date
    if [[ "$folder_date" =~ ^[0-9]{8}$ ]] && [[ "$folder_date" -lt "$cutoff_date" ]]; then
      echo "Removing: $folder"
      rm -rf "$folder"
    fi
  done

  echo "Cleanup completed."
}

# Function to create a temporary folder using mktemp
create_tmp_folder() {
  local base_dir=$1  # Base directory where the temporary folder will be created (required)
  local folder_prefix=$2  # Optional folder prefix (default: "tmp")

  # Validate required parameters
  if [ -z "$base_dir" ]; then
    echo "ERROR: Base directory is required for folder creation."
    exit 1
  fi

  # Default values for optional parameters
  folder_prefix=${folder_prefix:-tmp}
  ts_prefix=$(date +%Y%m%d_%H%M%S)

  # Ensure the "exec" directory exists, create it if it doesn't
  if [ ! -d "${base_dir}/${folder_prefix}" ]; then
    echo "Directory '${base_dir}/${folder_prefix}' does not exist. Creating it..."
    mkdir -p "${base_dir}/${folder_prefix}" || { echo "ERROR: Failed to create '${base_dir}/${folder_prefix}'"; exit 1; }
  fi

  # Create a unique temporary folder using mktemp
  TMP_FOLDER=$(mktemp -d "${base_dir}/${folder_prefix}/exec_${ts_prefix}_XXXXXXXXXXXX")

  # Check if mktemp succeeded
  if [ ! -d "$TMP_FOLDER" ]; then
    echo "ERROR: Failed to create temporary folder at $TMP_FOLDER"
    exit 1
  fi

  echo "Created temporary folder: $TMP_FOLDER"
}


# Function to copy files (but not folders) from source to temporary folder
copy_files_to_tmp_folder() {
  local source_dir=$1  # Source directory (required)

  # Validate required parameters
  if [ -z "$source_dir" ]; then
    echo "ERROR: Source directory is required for copying files."
    exit 1
  fi

  # Check if source directory exists
  if [ ! -d "$source_dir" ]; then
    echo "ERROR: Source directory does not exist: $source_dir"
    exit 1
  fi

  # Copy all files (but not folders) from $source_dir to $TMP_FOLDER using cp
  echo "Copying files from $source_dir to $TMP_FOLDER..."
  find "$source_dir" -maxdepth 1 -type f -exec cp {} "$TMP_FOLDER/" \; || {
    echo "ERROR: Failed to copy files from $source_dir to $TMP_FOLDER"
    exit 1
  }
}

# Function to clean up the temporary folder
cleanup_tmp_folder() {
  if [ -d "$TMP_FOLDER" ]; then
    echo "Cleaning up... Deleting temporary folder: $TMP_FOLDER"
    rm -rf "$TMP_FOLDER" || { echo "ERROR: Failed to delete $TMP_FOLDER"; exit 1; }
  fi
}

# Set the trap to ensure cleanup on script exit or interruption (SIGINT)
trap cleanup_tmp_folder EXIT SIGINT
