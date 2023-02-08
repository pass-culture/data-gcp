SQL_PATH = f"dependencies/contentful/sql/analytics"


contentful_tables = {
    "contentful_entries": {
        "sql": f"{SQL_PATH}/contentful_entries.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "contentful_entries",
    },
    "contentful_tags": {
        "sql": f"{SQL_PATH}/contentful_tags.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "contentful_tags",
    },
    "contentful_relationships": {
        "sql": f"{SQL_PATH}/contentful_relationships.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "contentful_relationships",
    },
    "contentful_homepages": {
        "sql": f"{SQL_PATH}/contentful_homepages.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "contentful_homepages",
        "dag_depends": [
            {"import_daily_firebase_data": "analytics_firebase_events"}
        ],  # dag_id: task_id
    },
    "contentful_algolia_modules_criterion": {
        "sql": f"{SQL_PATH}/contentful_algolia_modules_criterion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "contentful_algolia_modules_criterion",
        "depends": ["contentful_entries", "contentful_relationships"],
        "dag_depends": [{"import_analytics_v7": "end_import"}],  # dag_id: task_id
    },
}
