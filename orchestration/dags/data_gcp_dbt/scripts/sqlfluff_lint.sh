#!/bin/bash

# Default to "master" if no branch is specified
if [[ -z "$1" ]] || [[ "$1" = "--check" ]]; then
    TARGET_BRANCH="master"
else
    TARGET_BRANCH=$1
fi

# # Check if the --check flag is passed as any argument add  sqlfluff flag
# SQLFLUFF_FLAG=""
# if [[ "$*" == *"--check"* ]]; then
#     SQLFLUFF_FLAG="--annotation-level failure"
# fi


sqlfuff_lint_changed_sql() {
    git fetch origin $TARGET_BRANCH
    # common_ancestor=$(git merge-base --fork-point origin/master HEAD)
    # sqls=$(git diff $common_ancestor --name-only | grep 'orchestration/dags/data_gcp_dbt/' | grep '\.sql$')
    sqls=$(git diff origin/$TARGET_BRANCH --name-only | grep 'orchestration/dags/data_gcp_dbt/' | grep '\.sql$' | sed 's|orchestration/dags/data_gcp_dbt/||')

    if [[ -z "$sqls" ]]; then
        echo "no SQL files were modified"
    else
        existing_sqls=""
        for sql in $sqls; do
            if [[ -f "$sql" ]]; then
                # Remove the 'orchestration/dags/data_gcp_dbt/' part from the path
                relative_sql=$(echo "$sql" | sed 's|orchestration/dags/data_gcp_dbt/||')
                existing_sqls="$existing_sqls $relative_sql"
            else
                echo "Warning: Skipping non-existent file $sql"
            fi
        done
        echo "existing_sqls: $existing_sqls"

        if [[ -n "$existing_sqls" ]]; then
            sqlfluff lint $existing_sqls -p -1 --verbose
            # sqlfluff lint $existing_sqls -p -1 $SQLFLUFF_FLAG
        else
            echo "No existing SQL files to lint."
        fi
    fi
}

sqlfuff_lint_changed_sql
