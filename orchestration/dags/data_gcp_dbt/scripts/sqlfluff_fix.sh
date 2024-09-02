#!/bin/bash

sqlfuff_fix_changed_sql() {
    common_ancestor=$(git merge-base --fork-point origin/master HEAD)
    sqls=$(git diff $common_ancestor --name-only | grep 'orchestration/dags/data_gcp_dbt/' | grep '\.sql$')

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
            cd orchestration/dags/data_gcp_dbt && sqlfluff fix $existing_sqls -p -1
        else
            echo "No existing SQL files to form t."
        fi
    fi
}

sqlfuff_fix_changed_sql
