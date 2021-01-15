import ast
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.mysql_to_gcs import (
    MySqlToGoogleCloudStorageOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.bigquery_client import BigQueryClient
from dependencies.big_query_data_schema import TABLE_DATA
from dependencies.matomo_client import MatomoClient

GCP_PROJECT_ID = "pass-culture-app-projet-test"
GCS_BUCKET = "dump_scalingo"
BIGQUERY_DATASET = "algo_reco_kpi_matomo"


default_args = {
    "start_date": datetime(2021, 1, 9),
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "dump_scalingo_matomo_refresh_v2",
    default_args=default_args,
    description="Dump scalingo matomo new data to cloud storage in csv format and use it to refresh data in bigquery",
    schedule_interval="0 4 * * *",
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

MATOMO_CONNECTION_DATA = ast.literal_eval(os.environ.get("MATOMO_CONNECTION_DATA"))

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 10026

os.environ[
    "AIRFLOW_CONN_MYSQL_SCALINGO"
] = f"mysql://{MATOMO_CONNECTION_DATA.get('user')}:{MATOMO_CONNECTION_DATA.get('password')}@{LOCAL_HOST}:{LOCAL_PORT}/{MATOMO_CONNECTION_DATA.get('dbname')}"

bigquery_client = BigQueryClient(
    "/home/airflow/gcs/dags/pass-culture-app-projet-test-19edd3c79717.json"
)
matomo_client = MatomoClient(MATOMO_CONNECTION_DATA, LOCAL_PORT)

start = DummyOperator(task_id="start", dag=dag)
end_export = DummyOperator(task_id="end_export", dag=dag)
end_import = DummyOperator(task_id="end_import", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


def query_mysql_from_tunnel(**kwargs):
    tunnel = matomo_client.create_tunnel()
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

# we determine the date of the last visit action stored in bigquery
bigquery_query = f"SELECT max(visit_last_action_time) FROM `pass-culture-app-projet-test.{BIGQUERY_DATASET}.log_visit`"
timestamp = bigquery_client.query(bigquery_query).values[0][0]
# we add a margin of 3 hours
yesterday = (timestamp.to_pydatetime() + timedelta(hours=-3)).strftime(
    "%Y-%m-%d %H:%M:%S.%f"
)
# we will extract all visits those visit_last_action_time is older than this date

TABLE_DATA = {
    table: TABLE_DATA[table]
    for table in ["log_visit", "log_link_visit_action", "log_action"]
}

for table in TABLE_DATA:
    query_filter = (
        f"visit_last_action_time > TIMESTAMP '{yesterday}'"
        if table == "log_visit"
        else None
    )

    if table == "log_visit":
        bigquery_query = (
            f"SELECT max({TABLE_DATA[table]['id']}) "
            f"FROM `pass-culture-app-projet-test.{BIGQUERY_DATASET}.{table}` "
            f"where visit_last_action_time <= TIMESTAMP '{yesterday}'"
        )
        bigquery_result = bigquery_client.query(bigquery_query)
        min_id = int(bigquery_result.values[0][0])
    else:
        bigquery_query = (
            f"SELECT max({TABLE_DATA[table]['id']}) "
            f"FROM `pass-culture-app-projet-test.{BIGQUERY_DATASET}.{table}`"
        )
        bigquery_result = bigquery_client.query(bigquery_query)
        min_id = int(bigquery_result.values[0][0])

    matomo_query = f"SELECT max({TABLE_DATA[table]['id']}) FROM {table}"
    matomo_result = matomo_client.query(matomo_query)
    max_id = int(matomo_result[0][0])

    row_number_queried = TABLE_DATA[table]["row_number_queried"]
    query_count = 0
    for query_index in range(min_id, max_id, row_number_queried):
        sql_query = (
            f"select {', '.join([column['name'] for column in TABLE_DATA[table]['columns']])} "
            f"from {table} "
            f"where {TABLE_DATA[table]['id']} > {query_index} "
            f"and {TABLE_DATA[table]['id']} <= {query_index + row_number_queried}"
        )

        sql_query += f" and {query_filter};" if query_filter else ";"

        # File path and name.
        file_name = f"refresh/{table}/{now}_{query_count}_{'{}'}.csv"

        export_table = PythonOperator(
            task_id=f"query_{table}_{query_count}",
            python_callable=query_mysql_from_tunnel,
            op_kwargs={
                "table": table,
                "sql_query": sql_query,
                "file_name": file_name,
            },
            dag=dag,
        )
        last_task >> export_table
        last_task = export_table
        query_count += 1

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

preprocess_log_action_query = f"""
WITH filtered AS (
    SELECT *,
    REGEXP_EXTRACT(name, r"Module name: (.*) -") as module_name,
    REGEXP_EXTRACT(name, r"Number of tiles: ([0-9]*)") as number_tiles,
    REGEXP_EXTRACT(name, r"Offer id: ([A-Z0-9]*)") as offer_id,
    REGEXP_EXTRACT(name, r"details\/([A-Z0-9]{{4,5}})[^a-zA-Z0-9]") as offer_id_from_url,
    FROM {BIGQUERY_DATASET}.log_action
    WHERE type not in (1, 2, 3, 8)
    OR (type = 1 AND name LIKE "%.passculture.beta.gouv.fr/%")
),
dehumanized AS (
    SELECT
        *,
        IF( offer_id is not null,
            algo_reco_kpi_data.dehumanize_id(offer_id), null) AS dehumanize_offer_id,
        IF( offer_id_from_url is not null,
            algo_reco_kpi_data.dehumanize_id(offer_id_from_url), null) AS dehumanize_offer_id_from_url
    FROM filtered
)
SELECT
    STRUCT (idaction, name, _hash, type, url_prefix) as raw_data,
    STRUCT (module_name, number_tiles, offer_id, dehumanize_offer_id) AS tracker_data,
    STRUCT (offer_id_from_url as offer_id, dehumanize_offer_id_from_url as dehumanize_offer_id) AS url_data
FROM dehumanized;
"""

preprocess_log_action_task = BigQueryOperator(
    task_id="preprocess_log_action",
    sql=preprocess_log_action_query,
    destination_dataset_table=f"{BIGQUERY_DATASET}.log_action_preprocessed",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

preprocess_log_link_visit_action_query = f"""
SELECT
    idlink_va,
    idvisitor,
    idvisit,
    server_time,
    idaction_name,
    idaction_url,
    idaction_event_action,
    idaction_event_category
FROM {BIGQUERY_DATASET}.log_link_visit_action
"""

preprocess_log_link_visit_action_task = BigQueryOperator(
    task_id="preprocess_log_link_visit_action",
    sql=preprocess_log_link_visit_action_query,
    destination_dataset_table=f"{BIGQUERY_DATASET}.log_link_visit_action_preprocessed",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)


filter_log_link_visit_action_query = f"""
DELETE
FROM {BIGQUERY_DATASET}.log_link_visit_action_preprocessed as llvap
WHERE llvap.idlink_va NOT IN
(
    SELECT idlink_va
    FROM {BIGQUERY_DATASET}.log_link_visit_action_preprocessed as llvap
    LEFT OUTER JOIN {BIGQUERY_DATASET}.log_action_preprocessed as lap1
        ON lap1.raw_data.idaction = llvap.idaction_url
    LEFT OUTER JOIN {BIGQUERY_DATASET}.log_action_preprocessed as lap2
        ON lap2.raw_data.idaction = llvap.idaction_name
    WHERE
    ((lap1.raw_data.idaction IS NULL AND llvap.idaction_url is not null) OR llvap.idaction_url IS NULL)
    AND ((lap2.raw_data.idaction IS NULL AND llvap.idaction_name is not null) OR llvap.idaction_name IS NULL)
    AND llvap.idaction_event_action is null AND llvap.idaction_event_category is null
)
"""

filter_log_link_visit_action_task = BigQueryOperator(
    task_id="filter_log_link_visit_action",
    sql=filter_log_link_visit_action_query,
    use_legacy_sql=False,
    dag=dag,
)

filter_log_action_query = f"""
DELETE FROM {BIGQUERY_DATASET}.log_action_preprocessed as lap
WHERE lap.raw_data.idaction IN (
    SELECT lap.raw_data.idaction
    FROM {BIGQUERY_DATASET}.log_action_preprocessed as lap
    LEFT OUTER JOIN {BIGQUERY_DATASET}.log_link_visit_action_preprocessed as llvap1
        ON lap.raw_data.idaction = llvap1.idaction_url
    LEFT OUTER JOIN {BIGQUERY_DATASET}.log_link_visit_action_preprocessed as llvap2
        ON lap.raw_data.idaction = llvap2.idaction_name
    LEFT OUTER JOIN {BIGQUERY_DATASET}.log_link_visit_action_preprocessed as llvap3
        ON lap.raw_data.idaction = llvap3.idaction_event_action
    LEFT OUTER JOIN {BIGQUERY_DATASET}.log_link_visit_action_preprocessed as llvap4
        ON lap.raw_data.idaction = llvap4.idaction_event_category
    WHERE
        llvap1.idaction_url is null
    AND
        llvap2.idaction_url is null
    AND
        llvap3.idaction_url is null
    AND
        llvap4.idaction_url is null
)
"""

filter_log_action_task = BigQueryOperator(
    task_id="filter_log_action",
    sql=filter_log_action_query,
    use_legacy_sql=False,
    dag=dag,
)

end_import >> [
    preprocess_log_action_task,
    preprocess_log_link_visit_action_task,
] >> [filter_log_link_visit_action_task, filter_log_action_task] >> end
