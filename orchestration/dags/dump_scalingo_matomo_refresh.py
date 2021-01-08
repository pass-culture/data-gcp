import ast
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.contrib.operators.mysql_to_gcs import (
    MySqlToGoogleCloudStorageOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import bigquery
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(
    "/home/airflow/gcs/dags/pass-culture-app-projet-test-19edd3c79717.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCS_BUCKET = "dump_scalingo"
BIGQUERY_DATASET = "algo_reco_kpi_matomo"
TABLE_DATA = {
    "log_link_visit_action": {
        "id": "idlink_va",
        "columns": [
            {"name": "idlink_va", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idaction_url_ref", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_name_ref", "type": "INT64", "mode": "NULLABLE"},
            {"name": "custom_float", "type": "STRING", "mode": "NULLABLE"},  # INT64
            {"name": "server_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "idpageview", "type": "STRING", "mode": "NULLABLE"},
            {"name": "interaction_position", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "time_spent_ref_action", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_event_action", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_event_category", "type": "INT64", "mode": "NULLABLE"},
            {
                "name": "idaction_content_interaction",
                "type": "INT64",
                "mode": "NULLABLE",
            },
            {"name": "idaction_content_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_content_piece", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_content_target", "type": "INT64", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 300000,
    },
    "log_visit": {
        "id": "idvisit",
        "columns": [
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "visit_last_action_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "config_id", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "location_ip", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "visit_first_action_time",
                "type": "TIMESTAMP",
                "mode": "REQUIRED",
            },
            {"name": "visit_goal_buyer", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_goal_converted", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_first", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_order", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_returning", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_count_visits", "type": "INT64", "mode": "REQUIRED"},
            {"name": "visit_entry_idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_entry_idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_exit_idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_exit_idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_actions", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_interactions", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_searches", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_browser_lang", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_engine", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_os", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_os_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "visit_total_events", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_localtime", "type": "STRING", "mode": "NULLABLE"},  # TIME
            {"name": "visitor_days_since_last", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_resolution", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_cookie", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_director", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_flash", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_gears", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_java", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_pdf", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_quicktime", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_realplayer", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_silverlight", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_windowsmedia", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_time", "type": "INT64", "mode": "REQUIRED"},
            {"name": "location_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_content", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_provider", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 100000,
    },
    "log_action": {
        "id": "idaction",
        "columns": [
            {"name": "idaction", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "hash", "type": "INT64", "mode": "REQUIRED"},
            {"name": "type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "url_prefix", "type": "INT64", "mode": "NULLABLE"},
        ],
        "row_number_queried": 1000000,
    },
}


default_args = {
    "start_date": datetime(2021, 1, 7),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dump_scalingo_matomo_refresh_v1",
    default_args=default_args,
    description="Dump scalingo matomo db to cloud storage in csv format and import it in bigquery",
    schedule_interval="0 4 * * *",
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

MATOMO_CONNECTION_DATA = ast.literal_eval(os.environ.get("MATOMO_CONNECTION_DATA"))

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 10026

os.environ[
    "AIRFLOW_CONN_MYSQL_SCALINGO"
] = f"mysql://{MATOMO_CONNECTION_DATA['user']}:{MATOMO_CONNECTION_DATA['password']}@{LOCAL_HOST}:{LOCAL_PORT}/{MATOMO_CONNECTION_DATA['dbname']}"

start = DummyOperator(task_id="start", dag=dag)
end_export = DummyOperator(task_id="end_export", dag=dag)
end_import = DummyOperator(task_id="end_import", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


def create_tunnel():
    # Open SSH tunnel
    ssh_hook = SSHHook(
        ssh_conn_id="ssh_scalingo",
        keepalive_interval=120,
    )
    tunnel = ssh_hook.get_tunnel(
        remote_port=MATOMO_CONNECTION_DATA.get("port", 0),
        remote_host=MATOMO_CONNECTION_DATA.get("host", 0),
        local_port=LOCAL_PORT,
    )
    return tunnel


def query_mysql_from_tunnel(**kwargs):
    tunnel = create_tunnel()
    tunnel.start()

    extraction_task = MySqlToGoogleCloudStorageOperator(
        task_id=f"dump_{kwargs['table']}",
        sql=kwargs["sql_query"],
        bucket=GCS_BUCKET,
        filename=kwargs["file_name"],
        mysql_conn_id="mysql_scalingo",
        google_cloud_storage_conn_id="google_cloud_default",
        gzip=False,
        export_format="csv",
        field_delimiter=",",
        schema=None,
        dag=dag,
    )
    extraction_task.execute(context=kwargs)
    tunnel.stop()
    return


last_task = start
now = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() + timedelta(days=-1)).strftime("%Y-%m-%d")

for table in TABLE_DATA:
    if table == "log_visit":
        sql_query = (
            f"select {', '.join([column['name'] for column in TABLE_DATA[table]['columns']])} "
            f"from {table} "
            f"where visit_last_action_time > TIMESTAMP '{yesterday} 02:00:00.0';"
        )
    else:
        df = client.query(
            f"SELECT max({TABLE_DATA[table]['id']}) FROM `pass-culture-app-projet-test.{BIGQUERY_DATASET}.{table}`"
        ).to_dataframe()
        max_id = df.values[0][0]
        sql_query = (
            f"select {', '.join([column['name'] for column in TABLE_DATA[table]['columns']])} "
            f"from {table} "
            f"where {TABLE_DATA[table]['id']} > {max_id};"
        )

    # File path and name.
    file_name = f"refresh/{table}/{now}_{'{}'}.csv"

    export_table = PythonOperator(
        task_id=f"query_{table}",
        python_callable=query_mysql_from_tunnel,
        op_kwargs={"table": table, "sql_query": sql_query, "file_name": file_name},
        dag=dag,
    )
    last_task >> export_table
    last_task = export_table

last_task >> end_export

for table in TABLE_DATA:
    # Rename columns using bigQuery protected name
    protected_names = ["hash"]
    TABLE_DATA[table]["columns"] = [
        (
            column
            if column["name"] not in protected_names
            else {**column, "name": f"_{column['name']}"}
        )
        for column in TABLE_DATA[table]["columns"]
    ]

    if table == "log_visit":
        delete_task = BigQueryTableDeleteOperator(
            task_id=f"delete_{table}_in_bigquery",
            deletion_dataset_table=f"{GCP_PROJECT_ID}:{BIGQUERY_DATASET}.temp_{table}",
            ignore_if_missing=True,
            dag=dag,
        )
        create_empty_table_task = BigQueryCreateEmptyTableOperator(
            task_id=f"create_empty_{table}_in_bigquery",
            project_id=GCP_PROJECT_ID,
            dataset_id=BIGQUERY_DATASET,
            table_id=f"temp_{table}",
            schema_fields=TABLE_DATA[table]["columns"],
            dag=dag,
        )
        import_task = GoogleCloudStorageToBigQueryOperator(
            task_id=f"import_temp_{table}_in_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[f"refresh/{table}/{now}_*.csv"],
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.temp_{table}",
            write_disposition="WRITE_EMPTY",
            skip_leading_rows=1,
            schema_fields=TABLE_DATA[table]["columns"],
            autodetect=False,
            dag=dag,
        )
        delete_old_rows = BigQueryOperator(
            task_id=f"delete_old_{table}_rows",
            bql=f"SELECT * from {GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table} "
            f"where idvisit NOT IN (SELECT idvisit from {GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_{table})",
            destination_dataset_table=f"{BIGQUERY_DATASET}.{table}",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            dag=dag,
        )

        dehumanize_query = f"""
            SELECT
                *,
                IF(
                    REGEXP_CONTAINS(user_id, r"^[A-Z0-9]{2,}") = True,
                    algo_reco_kpi_data.dehumanize_id(REGEXP_EXTRACT(user_id, r"^[A-Z0-9]{2,}")),
                    ''
                )
                AS user_id_dehumanized,
            FROM
                {BIGQUERY_DATASET}.temp_log_visit;
        """
        dehumanize_user_id_task = BigQueryOperator(
            task_id="dehumanize_user_id",
            sql=dehumanize_query,
            destination_dataset_table=f"{BIGQUERY_DATASET}.temp_{table}",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            dag=dag,
        )
        add_new_rows = BigQueryOperator(
            task_id=f"add_new_{table}_rows",
            bql=f"SELECT * from {GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table} "
            f"UNION ALL (SELECT * from {GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_{table})",
            destination_dataset_table=f"{BIGQUERY_DATASET}.{table}",
            write_disposition="WRITE_TRUNCATE",
            use_legacy_sql=False,
            dag=dag,
        )
        end_export >> delete_task >> create_empty_table_task >> import_task >> delete_old_rows >> dehumanize_user_id_task >> add_new_rows >> end_import
    else:
        import_task = GoogleCloudStorageToBigQueryOperator(
            task_id=f"import_{table}_in_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[f"refresh/{table}/{now}_*.csv"],
            destination_project_dataset_table=f"{BIGQUERY_DATASET}.{table}",
            write_disposition="WRITE_APPEND",
            skip_leading_rows=1,
            schema_fields=TABLE_DATA[table]["columns"],
            autodetect=False,
            dag=dag,
        )
        end_export >> import_task >> end_import

dehumanize_log_action_query = f"""
SELECT
    *,
    algo_reco_kpi_data.dehumanize_id(offer_id) AS dehumanize_offer_id
FROM (
    SELECT
        *,
        IF(
            REGEXP_CONTAINS(name, r"app.*\/([A-Z0-9]{{4,5}})[^A-Za-z0-9]"),
            REGEXP_EXTRACT(name, r"\/([A-Z0-9]{{4,5}})"),
            ""
            ) AS offer_id,
        IF(
            REGEXP_CONTAINS(name, r"app.*"),
            REGEXP_EXTRACT(name, r"\/([a-z]*)"),
            ""
            ) AS base_page
    FROM
        {BIGQUERY_DATASET}.log_action
);
"""

dehumanize_log_action_task = BigQueryOperator(
    task_id="dehumanize_log_action",
    sql=dehumanize_log_action_query,
    destination_dataset_table=f"{BIGQUERY_DATASET}.log_action_processed",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

end_import >> dehumanize_log_action_task >> end
