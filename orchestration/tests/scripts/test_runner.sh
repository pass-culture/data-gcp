#!/bin/bash

JOB_PATH="$1"
if [ -z "$JOB_PATH" ]; then
    echo "Usage: $0 <job_path>"
    echo "Examples:"
    echo "  $0 jobs/etl_jobs/external/contentful"
    echo "  $0 jobs/etl_jobs/http_tools"
    exit 1
fi

# Extract job name and determine test directory structure
JOB_NAME=$(basename "$JOB_PATH")
VENV_NAME=".venv-test-$JOB_NAME"
TEST_EXIT_CODE=0

# Determine test directory based on job structure
if [[ "$JOB_PATH" == jobs/etl_jobs/external/* ]]; then
    # External job structure
    TEST_DIR="tests/unit/etl_jobs/external/$JOB_NAME"
elif [[ "$JOB_PATH" == jobs/etl_jobs/http_tools ]]; then
    # HTTP tools
    TEST_DIR="tests/unit/etl_jobs/http_tools"
elif [[ "$JOB_PATH" == jobs/etl_jobs/* ]]; then
    # Other direct etl_jobs subdirectory
    TEST_DIR="tests/unit/etl_jobs/$JOB_NAME"
else
    echo "❌ Unsupported job path structure: $JOB_PATH"
    exit 1
fi

# Cleanup function
cleanup() {
    echo "🧹 Cleaning up virtual environment: $VENV_NAME"
    if [ -d "$VENV_NAME" ]; then
        rm -rf "$VENV_NAME"
        echo "✨ Virtual environment removed"
    fi
}

# Set trap to cleanup on exit (success or failure)
trap cleanup EXIT

echo "🧪 Running tests for $JOB_NAME in isolated environment..."
echo "📁 Job path: $JOB_PATH"
echo "📁 Test directory: $TEST_DIR"

# Create isolated venv
echo "📦 Creating virtual environment: $VENV_NAME"
if ! uv venv "$VENV_NAME"; then
    echo "❌ Failed to create virtual environment"
    exit 1
fi

source "$VENV_NAME/bin/activate"

# Check if test directory exists first
if [ ! -d "$TEST_DIR" ]; then
    echo "❌ No unit tests found for $JOB_NAME"
    echo "Expected directory: $TEST_DIR"
    exit 1
fi

# Check if requirements.txt exists
if [ ! -f "$JOB_PATH/requirements.txt" ]; then
    echo "❌ No requirements.txt found in $JOB_PATH"
    echo "Please create a requirements.txt file for the job dependencies."
    exit 1
fi

# Install job requirements with error handling
echo "📥 Installing job requirements..."
if ! uv pip install -r "$JOB_PATH/requirements.txt"; then
    echo "❌ Failed to install job requirements"
    exit 1
fi

# Install test dependencies
echo "🔧 Installing test dependencies..."
if ! uv pip install pytest pytest-cov pytest-mock pytest-env; then
    echo "❌ Failed to install test dependencies"
    exit 1
fi

# Install job as editable package or add to PYTHONPATH
echo "📦 Setting up job package access..."

# Try to install as editable package first (if pyproject.toml exists)
if [ -f "$JOB_PATH/pyproject.toml" ]; then
    if uv pip install -e "$JOB_PATH" 2>/dev/null; then
        echo "✅ Job installed as editable package"
        JOB_PYTHONPATH="$JOB_PATH"
    else
        echo "❌ Failed to install job as editable package"
        exit 1
    fi
else
    echo "📝 No pyproject.toml found, adding job to PYTHONPATH"
    # For jobs without pyproject.toml, add to PYTHONPATH
    JOB_PYTHONPATH="$JOB_PATH:$(pwd)"

    # For http_tools specifically, also add the parent etl_jobs directory
    if [[ "$JOB_PATH" == jobs/etl_jobs/http_tools ]]; then
        JOB_PYTHONPATH="$(pwd)/jobs/etl_jobs:$JOB_PYTHONPATH"
        echo "✅ Added etl_jobs and http_tools to PYTHONPATH"
    fi

    # Verify key modules are accessible
    if [ -f "$JOB_PATH/main.py" ] || [ -f "$JOB_PATH/__init__.py" ] || [ -f "$JOB_PATH/clients.py" ]; then
        echo "✅ Found job modules in $JOB_PATH"
    else
        echo "❌ No recognizable job modules found in $JOB_PATH"
        echo "Expected at least one of: main.py, __init__.py, clients.py"
        exit 1
    fi
fi

# Run tests
echo "🚀 Running tests..."

# Set environment variables to prevent secret manager access during testing
export GOOGLE_APPLICATION_CREDENTIALS="/dev/null"
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="/dev/null"
export GOOGLE_CLOUD_PROJECT="test-project"

# Coverage configuration
COVERAGE_ARGS="--cov=$JOB_PATH --cov-report=html:htmlcov-$JOB_NAME --cov-report=term-missing --cov-fail-under=70"

if PYTHONPATH="$JOB_PYTHONPATH" pytest "$TEST_DIR" \
    $COVERAGE_ARGS \
    --tb=short \
    -v \
    "$@"; then
    echo "📊 Coverage report saved to htmlcov-$JOB_NAME/index.html"
    echo "✅ Tests completed successfully for $JOB_NAME"
    TEST_EXIT_CODE=0
else
    echo "❌ Tests failed for $JOB_NAME"
    TEST_EXIT_CODE=1
fi

# Exit with the test result code
exit $TEST_EXIT_CODE
