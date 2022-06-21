from common.config import DAG_FOLDER

SQL_PATH = f"{DAG_FOLDER}/dependencies/metabase/sql/raw"


import_tables = {
    "core_session": {
        "sql": f"{SQL_PATH}/core_session.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_core_session",
    },
    "core_user": {
        "sql": f"{SQL_PATH}/core_user.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_core_user",
    },
    "login_history": {
        "sql": f"{SQL_PATH}/login_history.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_login_history",
    },
    "query_execution": {
        "sql": f"{SQL_PATH}/query_execution.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_query_execution",
    },
    "query": {
        "sql": f"{SQL_PATH}/query.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_query",
    },
    "report_card": {
        "sql": f"{SQL_PATH}/report_card.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_report_card",
    },
    "report_dashboard": {
        "sql": f"{SQL_PATH}/report_dashboard.sql",
        "destination_dataset_table": "{{ bigquery_raw_dataset }}.metabase_report_dashboard",
    },
}


def from_external(conn_id, params):
    with open(params["sql"], "r") as f:
        sql_query = '"""{}"""'.format(f.read())
        return f"""SELECT * FROM EXTERNAL_QUERY(
                "{conn_id}",
                {sql_query}
            );"""
