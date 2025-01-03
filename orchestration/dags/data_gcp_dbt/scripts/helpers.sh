#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

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

  # Create a unique temporary folder using mktemp
  TMP_FOLDER=$(mktemp -d "${base_dir}/${folder_prefix}_XXXXXXXXXXXX")

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
