#!/bin/bash
# Check if manifest.json exists
if [ -f "target/manifest.json" ]; then
    # Run the Python script to hide dbt package documentation
    python scripts/hide_macros.py
    echo "dbt package visibility updated in manifest.json."
else
    echo "Error: manifest.json not found. Make sure dbt has successfully compiled."
fi

dbt docs generate && dbt docs serve
