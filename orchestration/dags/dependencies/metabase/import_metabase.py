from common.config import DAG_FOLDER

SQL_PATH = f"dependencies/metabase/sql"


import_tables = {
    "core_session": {
        "sql": f"{SQL_PATH}/raw/core_session.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_core_session",
    },
    "core_user": {
        "sql": f"{SQL_PATH}/raw/core_user.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_core_user",
    },
    "login_history": {
        "sql": f"{SQL_PATH}/raw/login_history.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_login_history",
    },
    "query_execution": {
        "sql": f"{SQL_PATH}/raw/query_execution.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_query_execution",
    },
    "query": {
        "sql": f"{SQL_PATH}/raw/query.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_query",
    },
    "report_card": {
        "sql": f"{SQL_PATH}/raw/report_card.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_report_card",
    },
    "report_dashboard": {
        "sql": f"{SQL_PATH}/raw/report_dashboard.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_report_dashboard",
    },
    "collections": {
        "sql": f"{SQL_PATH}/raw/collections.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_collections",
    },
    "view_log": {
        "sql": f"{SQL_PATH}/raw/view_logs.sql",
        "destination_dataset": "{{ bigquery_raw_dataset }}",
        "destination_table": "metabase_view_logs",
    },
}

analytics_tables = {
    "metabase_costs": {
        "sql": f"{SQL_PATH}/analytics/metabase_costs.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "metabase_costs",
    },
    "metabase_views": {
        "sql": f"{SQL_PATH}/analytics/metabase_views.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "metabase_views",
    },
    "metabase_activity": {
        "sql": f"{SQL_PATH}/analytics/metabase_activity.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "metabase_activity",
        "depends": ["ref_collections_archive"],
    },
    "ref_collections_archive": {
        "sql": f"{SQL_PATH}/analytics/ref_collections_archive.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "metabase_ref_collections_archive",
    },
}


def from_external(conn_id, params):
    with open(f"{DAG_FOLDER}/{params['sql']}", "r") as f:
        sql_query = '"""{}"""'.format(f.read())
        return f"""SELECT * FROM EXTERNAL_QUERY(
                "{conn_id}",
                {sql_query}
            );"""
