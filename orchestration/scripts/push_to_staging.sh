#!/usr/bin/env bash

# push_to_staging.sh
# Upload DAG files to Airflow Staging GCS bucket
# Usage: ./push_to_staging.sh [--dry-run] <dag_file_path>

set -euo pipefail

# Configuration
GCS_BUCKET="gs://airflow-data-bucket-stg"
REQUIRED_PREFIX="orchestration/dags/"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Functions
print_error() {
    echo -e "${RED}❌ Error: $1${NC}" >&2
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] <dag_file_path>

Upload DAG files to Airflow Staging GCS bucket.

Arguments:
  <dag_file_path>    Path to the DAG file (relative or absolute)
                     Must be inside orchestration/dags/ directory

Options:
  --dry-run          Preview the upload command without executing
  -h, --help         Show this help message

Examples:
  # Upload a DAG file
  $0 orchestration/dags/jobs/administration/my_dag.py

  # Dry-run to preview
  $0 --dry-run orchestration/dags/jobs/administration/my_dag.py

  # Works with absolute paths
  $0 /full/path/to/data-gcp/orchestration/dags/jobs/administration/my_dag.py

GCS Destination:
  Files are uploaded to: ${GCS_BUCKET}/dags/<path/to/dag>/<dag_name>.py
  The 'orchestration/' prefix is stripped from the local path.

EOF
}

# Parse arguments
DRY_RUN=false
DAG_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        -*)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [[ -n "$DAG_FILE" ]]; then
                print_error "Multiple file paths provided. Only one file can be uploaded at a time."
                show_usage
                exit 1
            fi
            DAG_FILE="$1"
            shift
            ;;
    esac
done

# Validate DAG_FILE was provided
if [[ -z "$DAG_FILE" ]]; then
    print_error "Missing required argument: <dag_file_path>"
    show_usage
    exit 1
fi

# Get git root directory
GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
if [[ $? -ne 0 ]]; then
    print_error "Not in a git repository"
    exit 1
fi

# Convert to absolute path if relative
if [[ "$DAG_FILE" != /* ]]; then
    DAG_FILE="$PWD/$DAG_FILE"
fi

# Normalize path (resolve .., ., etc.)
DAG_FILE=$(cd "$(dirname "$DAG_FILE")" && pwd)/$(basename "$DAG_FILE")

# Check if file exists
if [[ ! -f "$DAG_FILE" ]]; then
    print_error "File does not exist: $DAG_FILE"
    exit 1
fi

# Get relative path from git root
RELATIVE_PATH="${DAG_FILE#$GIT_ROOT/}"

# Validate file is inside orchestration/dags/
if [[ ! "$RELATIVE_PATH" =~ ^orchestration/dags/ ]]; then
    print_error "File must be inside orchestration/dags/ directory"
    print_info "Current path: $RELATIVE_PATH"
    print_info "Expected pattern: orchestration/dags/..."
    exit 1
fi

# Transform path for GCS (strip 'orchestration/' prefix)
# orchestration/dags/jobs/admin/dag.py -> dags/jobs/admin/dag.py
GCS_PATH="${RELATIVE_PATH#orchestration/}"
GCS_DESTINATION="${GCS_BUCKET}/${GCS_PATH}"

# Display upload information
echo ""
if [[ "$DRY_RUN" == true ]]; then
    print_info "DRY-RUN MODE: No files will be uploaded"
    echo ""
fi

echo "🚀 Uploading file to staging environment..."
echo "From (Local) : $RELATIVE_PATH"
echo "To   (GCS)   : $GCS_DESTINATION"
echo "------------------------------------------------------------"

# Execute or preview the upload
if [[ "$DRY_RUN" == true ]]; then
    echo ""
    print_info "Would execute: gcloud storage cp \"$DAG_FILE\" \"$GCS_DESTINATION\""
    echo ""
    print_success "Dry-run complete. Run without --dry-run to upload."
else
    # Check if gcloud is available
    if ! command -v gcloud &> /dev/null; then
        print_error "gcloud command not found. Please install Google Cloud SDK."
        exit 1
    fi

    # Perform the upload
    if gcloud storage cp "$DAG_FILE" "$GCS_DESTINATION"; then
        echo ""
        print_success "Upload complete!"
        echo ""
        print_info "The DAG should appear in Airflow Staging UI within 2-3 minutes."
        print_info "Airflow Staging: https://airflow-stg.data.ehp.passculture.team/home"
    else
        echo ""
        print_error "Upload failed. Check gcloud permissions and GCS bucket access."
        exit 1
    fi
fi

echo ""
