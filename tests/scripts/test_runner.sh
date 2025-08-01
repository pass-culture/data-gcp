#!/bin/bash

JOB_PATH="$1"
if [ -z "$JOB_PATH" ]; then
    echo "Usage: $0 <job_path>"
    echo "Example: $0 jobs/etl_jobs/external/contentful"
    exit 1
fi

JOB_NAME=$(basename "$JOB_PATH")
VENV_NAME=".venv-test-$JOB_NAME"
TEST_EXIT_CODE=0

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

# Create isolated venv
echo "📦 Creating virtual environment: $VENV_NAME"
if ! uv venv "$VENV_NAME"; then
    echo "❌ Failed to create virtual environment"
    exit 1
fi

source "$VENV_NAME/bin/activate"

# Check if test directory exists first
if [ ! -d "tests/unit/etl_jobs/external/$JOB_NAME" ]; then
    echo "❌ No unit tests found for $JOB_NAME"
    echo "Expected directory: tests/unit/etl_jobs/external/$JOB_NAME"
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

# Try to install as editable package first
if uv pip install -e "$JOB_PATH" 2>/dev/null; then
    echo "✅ Job installed as editable package"
    JOB_PYTHONPATH="$JOB_PATH"
else
    echo "📝 Job has flat layout, adding to PYTHONPATH instead"
    # For flat-layout jobs, we'll just add the job directory to PYTHONPATH
    JOB_PYTHONPATH="$JOB_PATH:$(pwd)"

    # Verify the job's main modules are accessible
    if [ -f "$JOB_PATH/main.py" ]; then
        echo "✅ Found main.py in job directory"
    else
        echo "❌ No main.py found in $JOB_PATH"
        exit 1
    fi
fi

# Run tests
echo "🚀 Running tests..."
# Set environment variables to prevent secret manager access during testing
export GOOGLE_APPLICATION_CREDENTIALS="/dev/null"
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="/dev/null"
export GOOGLE_CLOUD_PROJECT="test-project"

if PYTHONPATH="$JOB_PYTHONPATH" pytest "tests/unit/etl_jobs/external/$JOB_NAME" \
    --tb=short \
    -v; then
    echo "📊 Coverage report saved to htmlcov-$JOB_NAME/index.html"
    echo "✅ Tests completed successfully for $JOB_NAME"
    TEST_EXIT_CODE=0
else
    echo "❌ Tests failed for $JOB_NAME"
    TEST_EXIT_CODE=1
fi

# Exit with the test result code
exit $TEST_EXIT_CODE
