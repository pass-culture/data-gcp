#!/bin/bash

# Default to "master" if no branch is specified
if [ -z "$1" ] || [ "$1" == "--check" ]; then
    TARGET_BRANCH="master"
else
    TARGET_BRANCH=$1
fi

sqlfuff_format_changed_sql() {
    git fetch origin $TARGET_BRANCH
    # common_ancestor=$(git merge-base --fork-point origin/master HEAD)
    # sqls=$(git diff $common_ancestor --name-only | grep 'orchestration/dags/data_gcp_dbt/' | grep '\.sql$')
    sqls=$(git diff origin/$TARGET_BRANCH --name-only | grep 'orchestration/dags/data_gcp_dbt/' | grep '\.sql$' | sed 's|orchestration/dags/data_gcp_dbt/||')
    if [ -z "$sqls" ]; then
        echo "no SQL files were modified"
    else
        existing_sqls=""
        for sql in $sqls; do
            if [ -f "$sql" ]; then
                # Remove the 'orchestration/dags/data_gcp_dbt/' part from the path
                relative_sql=$(echo "$sql" | sed 's|orchestration/dags/data_gcp_dbt/||')
                existing_sqls="$existing_sqls $relative_sql"
            else
                echo "Warning: Skipping non-existent file $sql"
            fi
        done

        if [ -n "$existing_sqls" ]; then
            sqlfluff format $existing_sqls -p -1
        else
            echo "No existing SQL files to format."
        fi
    fi
}

sqlfuff_format_changed_sql
