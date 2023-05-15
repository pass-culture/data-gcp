ANALYTICS_SQL_PATH = f"dependencies/batch/sql/analytics"

import_batch_tables = {
    "batch_transactions": {
        "sql": f"{ANALYTICS_SQL_PATH}/batch_transactions.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "batch_transactions",
    },
    "batch_campaigns": {
        "sql": f"{ANALYTICS_SQL_PATH}/batch_campaigns.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "batch_campaigns",
    },
}
