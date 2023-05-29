import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLImportInstanceOperator,
    CloudSQLExecuteQueryOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

from common.access_gcp_secrets import access_secret_data
from common.compose_gcs_files import compose_gcs_files
from common.config import (
    GCP_PROJECT_ID,
    GCP_REGION,
    DATA_GCS_BUCKET_NAME,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    DAG_FOLDER,
    QPI_TABLE,
    RECOMMENDATION_SQL_INSTANCE,
)
from dependencies.import_recommendation_cloudsql.monitor_tables import monitoring_tables
from common.alerts import task_fail_slack_alert
from common import macros
from common.utils import get_airflow_schedule


database_url = access_secret_data(
    GCP_PROJECT_ID, f"{RECOMMENDATION_SQL_INSTANCE}_database_url", default=""
)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    database_url.replace("postgresql://", "gcpcloudsql://")
    + f"?database_type=postgres&project_id={GCP_PROJECT_ID}&location={GCP_REGION}&instance={RECOMMENDATION_SQL_INSTANCE}&use_proxy=True&sql_proxy_use_tcp=True"
)


TABLES_DATA_PATH = f"{DAG_FOLDER}/ressources/tables.csv"
BUCKET_NAME = "bigquery_exports/{{ ds }}"
BUCKET_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/{BUCKET_NAME}"
SQL_PATH = "dependencies/import_recommendation_cloudsql/sql"

default_args = {
    "start_date": datetime(2020, 12, 1),
    "retries": 3,
    "on_failure_callback": task_fail_slack_alert,
    "retry_delay": timedelta(minutes=5),
}


def get_table_data():
    data = {}
    tables = pd.read_csv(TABLES_DATA_PATH)
    for table_name in set(tables["table_name"].values):
        data[table_name] = {}
        table_data = tables.loc[lambda df: df.table_name == table_name]
        data[table_name]["columns"] = {
            column_name: data_type
            for column_name, data_type in zip(
                list(table_data.column_name.values), list(table_data.data_type.values)
            )
        }
        for additional_data in ["dataset_type", "bigquery_table_name"]:
            data[table_name][additional_data] = tables.loc[
                lambda df: df.table_name == table_name
            ][additional_data].values[0]
    return data


TABLES = get_table_data()


def get_table_names():
    tables = pd.read_csv(TABLES_DATA_PATH)
    table_names = tables.table_name.unique()
    return table_names


with DAG(
    "recommendation_cloud_sql_v1",
    default_args=default_args,
    description="Export bigQuery tables to GCS to dump and restore Cloud SQL tables",
    schedule_interval=get_airflow_schedule("15 5 * * *"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=480),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
) as dag:

    start = DummyOperator(task_id="start")
    start_monitoring = DummyOperator(task_id="start_monitoring")
    start_drop_restore = DummyOperator(task_id="start_drop_restore")

    end_monitoring = DummyOperator(task_id="end_monitoring")
    end_data_prep = DummyOperator(task_id="end_data_prep")

    monitor_tables_task = []
    for table, params in monitoring_tables.items():
        dataset = params["destination_dataset"]
        task = BigQueryExecuteQueryOperator(
            task_id=f"monitor_{table}",
            sql=params["sql"],
            destination_dataset_table=f"{GCP_PROJECT_ID}.{dataset}.monitor_{table}",
            use_legacy_sql=False,
            write_disposition="WRITE_APPEND",
        )
        monitor_tables_task.append(task)

    for table in TABLES:

        dataset_type = TABLES[table]["dataset_type"]
        bigquery_table_name = TABLES[table]["bigquery_table_name"]
        if dataset_type == "clean":
            dataset = BIGQUERY_CLEAN_DATASET
        if dataset_type == "analytics":
            dataset = BIGQUERY_ANALYTICS_DATASET

        list_type_columns = [
            column_name
            for column_name in TABLES[table]["columns"]
            if "[]" in TABLES[table]["columns"][column_name]
        ]

        select_columns = ", ".join(
            [
                f"`{column_name}`"
                for column_name in TABLES[table]["columns"]
                if column_name not in list_type_columns
            ]
        )
        if table == "qpi_answers":
            filter_column_query = f"""
                SELECT {select_columns}
                FROM `{GCP_PROJECT_ID}.{dataset}.enriched_{QPI_TABLE}`
            """
        else:
            filter_column_query = f"""
                SELECT {select_columns}
                FROM `{GCP_PROJECT_ID}.{dataset}.{bigquery_table_name}`
            """

        filter_column_task = BigQueryExecuteQueryOperator(
            task_id=f"filter_column_{table}",
            sql=filter_column_query,
            use_legacy_sql=False,
            destination_dataset_table=f"{GCP_PROJECT_ID}.{dataset}.temp_export_{table}",
            write_disposition="WRITE_TRUNCATE",
        )

        typed_columns = ", ".join(
            [
                f'"{col}" {TABLES[table]["columns"][col]}'
                for col in TABLES[table]["columns"]
                if "[]" not in TABLES[table]["columns"][col]
            ]
        )

        export_task = BigQueryInsertJobOperator(
            task_id=f"export_{table}_to_gcs",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": dataset,
                        "tableId": f"temp_export_{table}",
                    },
                    "compression": None,
                    "destinationUris": f"{BUCKET_PATH}/{table}-*.csv",
                    "destinationFormat": "CSV",
                    "printHeader": False,
                }
            },
            dag=dag,
        )

        delete_temp_table_task = BigQueryDeleteTableOperator(
            task_id=f"delete_temp_export_{table}_in_bigquery",
            deletion_dataset_table=f"{GCP_PROJECT_ID}.{dataset}.temp_export_{table}",
            ignore_if_missing=True,
        )

        compose_files_task = PythonOperator(
            task_id=f"compose_files_{table}",
            python_callable=compose_gcs_files,
            op_kwargs={
                "bucket_name": DATA_GCS_BUCKET_NAME,
                "source_prefix": f"{BUCKET_NAME}/{table}-",
                "destination_blob_name": f"{BUCKET_NAME}/{table}.csv",
            },
            dag=dag,
        )

        drop_table_task = CloudSQLExecuteQueryOperator(
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            task_id=f"drop_table_public_{table}",
            sql=f"DROP TABLE IF EXISTS public.{table};",
            autocommit=True,
        )

        create_table_task = CloudSQLExecuteQueryOperator(
            task_id=f"create_table_public_{table}",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"CREATE TABLE IF NOT EXISTS public.{table} ({typed_columns});",
            autocommit=True,
        )

        start_drop_restore >> filter_column_task
        (
            filter_column_task
            >> export_task
            >> delete_temp_table_task
            >> compose_files_task
        )
        compose_files_task >> drop_table_task >> create_table_task >> end_data_prep

    def create_restore_task(table_name: str):
        import_body = {
            "importContext": {
                "fileType": "CSV",
                "csvImportOptions": {"table": f"public.{table_name}"},
                "uri": f"{BUCKET_PATH}/{table_name}.csv",
                "database": RECOMMENDATION_SQL_INSTANCE,
            }
        }

        sql_restore_task = CloudSQLImportInstanceOperator(
            task_id=f"cloud_sql_restore_table_{table_name}",
            project_id=GCP_PROJECT_ID,
            body=import_body,
            instance=RECOMMENDATION_SQL_INSTANCE,
        )
        return sql_restore_task

    table_names = get_table_names()
    restore_tasks = []

    for index, table_name in enumerate(table_names):
        task = create_restore_task(table_name)
        restore_tasks.append(task)
        if index:
            restore_tasks[index - 1] >> restore_tasks[index]
    end_drop_restore = DummyOperator(task_id="end_drop_restore")

    end_data_prep >> restore_tasks[0]
    restore_tasks[-1] >> end_drop_restore
    # keep creation time in the format yymmddHHMM for the deletion step
    create_materialized_view = CloudSQLExecuteQueryOperator(
        task_id="create_materialized_view_recommendable_offers_per_iris_shape_mv",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql=f"{SQL_PATH}/create_recommendable_offers_per_iris_shape_tmp_mv.sql",
        autocommit=True,
    )

    create_materialized_raw_view = CloudSQLExecuteQueryOperator(
        task_id="create_materialized_raw_view_recommendable_offers_raw_mv",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql=f"{SQL_PATH}/create_recommendable_offers_raw_mv.sql",
        autocommit=True,
    )

    recreate_indexes_query = """
        CREATE INDEX IF NOT EXISTS idx_user_id                             ON public.enriched_user                        USING btree (user_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_offer_recommendable_id       ON public.recommendable_offers_per_iris_shape  USING btree (is_geolocated,iris_id,item_id,offer_id,unique_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_non_recommendable_id         ON public.non_recommendable_offers             USING btree (user_id,offer_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_enriched_user_mv             ON public.enriched_user_mv                     USING btree (user_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_qpi_answers_mv               ON public.qpi_answers_mv                       USING btree (user_id,subcategories);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_item_ids_mv                  ON public.item_ids_mv                          USING btree (offer_id);

    """
    recreate_indexes_task = CloudSQLExecuteQueryOperator(
        task_id="recreate_indexes",
        gcp_cloudsql_conn_id="proxy_postgres_tcp",
        sql=recreate_indexes_query,
        autocommit=True,
    )

    views_to_refresh = [
        "non_recommendable_offers",
        "enriched_user_mv",
        "qpi_answers_mv",
        "item_ids_mv",
    ]

    refresh_materialized_view_tasks = []
    for materialized_view in views_to_refresh:
        refresh_materialized_view_task = CloudSQLExecuteQueryOperator(
            task_id=f"refresh_{materialized_view}",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"REFRESH MATERIALIZED VIEW CONCURRENTLY {materialized_view};",
            autocommit=True,
        )
        refresh_materialized_view_tasks.append(refresh_materialized_view_task)

    # update MV
    end_refresh = DummyOperator(task_id="end_refresh")

    rename_materialized_view_tables = [
        "recommendable_offers_per_iris_shape_mv",
        "recommendable_offers_raw_mv",
    ]
    rename_materialized_view_tasks = []
    for materialized_view in rename_materialized_view_tables:
        rename_current_materialized_view = CloudSQLExecuteQueryOperator(
            task_id=f"rename_{materialized_view}_current_materialized_view_to_old",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"ALTER MATERIALIZED VIEW IF EXISTS {materialized_view} rename to {materialized_view}_old",
            autocommit=True,
        )
        rename_current_materialized_view.set_upstream(end_refresh)

        rename_temp_materialized_view = CloudSQLExecuteQueryOperator(
            task_id=f"rename_{materialized_view}_temp_materialized_view_to_current",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"ALTER MATERIALIZED VIEW {materialized_view}_tmp rename to {materialized_view}",
            autocommit=True,
        )
        rename_temp_materialized_view.set_upstream(rename_current_materialized_view)
        drop_old_materialized_view = CloudSQLExecuteQueryOperator(
            task_id=f"drop_{materialized_view}_old_materialized_view",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"DROP MATERIALIZED VIEW IF EXISTS {materialized_view}_old CASCADE",
            autocommit=True,
        )
        drop_old_materialized_view.set_upstream(rename_temp_materialized_view)

        # yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
        drop_old_function = CloudSQLExecuteQueryOperator(
            task_id=f"drop_{materialized_view}_old_function",
            gcp_cloudsql_conn_id="proxy_postgres_tcp",
            sql=f"DROP FUNCTION IF EXISTS get_{materialized_view}_"
            + "{{ prev_execution_date_success | ts_nodash }} CASCADE",
            autocommit=True,
        )
        drop_old_function.set_upstream(drop_old_materialized_view)
        rename_materialized_view_tasks.append(drop_old_function)

    end = DummyOperator(task_id="end")

    (
        start
        >> start_monitoring
        >> monitor_tables_task
        >> end_monitoring
        >> start_drop_restore
    )
    (
        end_drop_restore
        >> create_materialized_view
        >> create_materialized_raw_view
        >> recreate_indexes_task
        >> refresh_materialized_view_tasks
        >> end_refresh
    )
    (rename_materialized_view_tasks >> end)
