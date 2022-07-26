from common.config import DAG_FOLDER

SQL_PATH = f"dependencies/metabase/sql"


import_tables = {
    "core_session": {
        "sql": f"{SQL_PATH}/raw/core_session.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_core_session",
    },
    "core_user": {
        "sql": f"{SQL_PATH}/raw/core_user.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_core_user",
    },
    "login_history": {
        "sql": f"{SQL_PATH}/raw/login_history.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_login_history",
    },
    "query_execution": {
        "sql": f"{SQL_PATH}/raw/query_execution.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_query_execution",
    },
    "query": {
        "sql": f"{SQL_PATH}/raw/query.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_query",
    },
    "report_card": {
        "sql": f"{SQL_PATH}/raw/report_card.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_report_card",
    },
    "report_dashboard": {
        "sql": f"{SQL_PATH}/raw/report_dashboard.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_report_dashboard",
    },
}

analytics_tables = {
    "metabase_costs": {
        "sql": f"{SQL_PATH}/analytics/metabase_costs.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.metabase_costs",
    },
    "metabase_views": {
        "sql": f"{SQL_PATH}/analytics/metabase_views.sql",
        "destination_dataset_table": "{{ bigquery_analytics_dataset }}.metabase_views",
    },
}


def from_external(conn_id, params):
    with open(f"{DAG_FOLDER}/{params['sql']}", "r") as f:
        sql_query = '"""{}"""'.format(f.read())
        return f"""SELECT * FROM EXTERNAL_QUERY(
                "{conn_id}",
                {sql_query}
            );"""
