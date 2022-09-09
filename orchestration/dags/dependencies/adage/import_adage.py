SQL_PATH = f"dependencies/adage/sql/analytics"


analytics_tables = {
    "adage_involved_student": {
        "sql": f"{SQL_PATH}/adage_involved_student.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "adage_involved_student",
    },
}
