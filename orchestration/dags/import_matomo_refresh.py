import ast
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dependencies.bigquery_client import BigQueryClient
from dependencies.data_analytics.enriched_data.enriched_matomo import (
    aggregate_matomo_offer_events,
    aggregate_matomo_user_events,
)
from dependencies.matomo_data_schema import TABLE_DATA
from dependencies.matomo_sql_queries import (
    preprocess_log_visit_query,
    preprocess_log_visit_referer_query,
    preprocess_log_visit_config_query,
    preprocess_log_visit_location_query,
    preprocess_log_visit_campaign_query,
    preprocess_log_visit_custom_var_query,
    transform_matomo_events_query,
    add_screen_view_matomo_events_query,
    copy_events_to_analytics,
    copy_matomo_visits_to_analytics,
)
from dependencies.slack_alert import task_fail_slack_alert
from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    MATOMO_EXTERNAL_CONNECTION_ID,
)

ENV = os.environ.get("ENV")


def build_query_function(table_name):
    bigquery_client = BigQueryClient()
    if table_name in ["log_visit", "log_conversion"]:
        # we determine the date of the last visit action stored in bigquery
        bigquery_query = f"SELECT max({TABLE_DATA[table_name]['time_column']}) FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}`"
        timestamp = bigquery_client.query(bigquery_query).values[0][0]
        # we add a margin of 3 hours
        yesterday = (timestamp.to_pydatetime() + timedelta(hours=-3)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        # we will extract all visits those visit_last_action_time is older than this date
        query_filter = (
            f'AND {TABLE_DATA[table_name]["time_column"]} > TIMESTAMP "{yesterday}"'
        )
    else:
        query_filter = ""

    if table_name in ["log_visit", "log_conversion"]:
        bigquery_query = f"""
            SELECT max({TABLE_DATA[table_name]['id']})
            FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}`
            WHERE {TABLE_DATA[table_name]['time_column']} <= TIMESTAMP "{yesterday}"
        """
        bigquery_result = bigquery_client.query(bigquery_query)
        min_id = int(bigquery_result.values[0][0])

    elif table_name == "goal":
        min_id = 0
    else:
        bigquery_query = (
            f"SELECT max({TABLE_DATA[table_name]['id']}) "
            f"FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{table_name}`"
        )
        bigquery_result = bigquery_client.query(bigquery_query)
        min_id = int(bigquery_result.values[0][0])

    table_query = f"""
    SELECT *
    FROM {table_name}
    WHERE {TABLE_DATA[table_name]['id']} > {min_id}
    {query_filter};
    """
    one_line_query = " ".join([line.strip() for line in table_query.splitlines()])

    full_query = f"""
    SELECT {TABLE_DATA[table_name]["conversions"]} FROM EXTERNAL_QUERY('{MATOMO_EXTERNAL_CONNECTION_ID}', '{one_line_query}');
    """
    return full_query


default_args = {
    "start_date": datetime(2021, 4, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# DAG is launched once a week on dev env to have enough data to import
# on other env it is launched once a day
dag = DAG(
    "import_matomo_refresh_v1",
    default_args=default_args,
    description="Import Matomo data to Bigquery",
    schedule_interval="0 4 * * *" if ENV != "dev" else "0 4 * * 1",
    on_failure_callback=task_fail_slack_alert,
    dagrun_timeout=timedelta(minutes=180),
    catchup=False,
)

start = DummyOperator(task_id="start", dag=dag)

end_import = DummyOperator(task_id="end_import", dag=dag)

now = datetime.now().strftime("%Y-%m-%d")

first_import_tasks = []
last_import_tasks = []

for table in TABLE_DATA:

    build_query = PythonOperator(
        task_id=f"build_query_{table}",
        python_callable=build_query_function,
        op_kwargs={
            "table_name": table,
        },
        dag=dag,
    )

    import_table = BigQueryOperator(
        task_id=f"import_to_raw_{table}",
        sql="{{task_instance.xcom_pull(task_ids='build_query_"
        + table
        + "', key='return_value')}}",
        write_disposition="WRITE_TRUNCATE" if table == "goal" else "WRITE_APPEND",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_RAW_DATASET}.{table}",
        dag=dag,
    )

    build_query >> import_table
    first_import_tasks.append(build_query)

    # For two tables we need to remove duplicates.
    if table in ["log_visit", "log_conversion"]:
        delete_old_rows = BigQueryOperator(
            task_id=f"delete_old_rows_{table}",
            sql=f"""
            DELETE FROM `{BIGQUERY_RAW_DATASET}.{table}` as lv
            WHERE EXISTS (
                SELECT * except (row_number) FROM (
                    SELECT
                        *,
                        ROW_NUMBER()
                            OVER (PARTITION BY {TABLE_DATA[table]['id']} ORDER BY {TABLE_DATA[table]['time_column']} DESC)
                            AS row_number
                    FROM (
                        SELECT DISTINCT {TABLE_DATA[table]['id']}, {TABLE_DATA[table]['time_column']}
                        FROM `{BIGQUERY_RAW_DATASET}.{table}`
                        )
                ) as base
                WHERE base.row_number > 1
                AND lv.{TABLE_DATA[table]['id']} = base.{TABLE_DATA[table]['id']}
                AND lv.{TABLE_DATA[table]['time_column']} = base.{TABLE_DATA[table]['time_column']}
            );
            """,
            use_legacy_sql=False,
            dag=dag,
        )
        import_table >> delete_old_rows
        last_import_tasks.append(delete_old_rows)
    else:
        last_import_tasks.append(import_table)


start >> first_import_tasks
last_import_tasks >> end_import


end_preprocess = DummyOperator(task_id="end_preprocess", dag=dag)


matomo_visits_start = DummyOperator(task_id="matomo_visits_start", dag=dag)

normalize_log_visit_queries = {
    "": preprocess_log_visit_query(GCP_PROJECT, BIGQUERY_RAW_DATASET),
    "_referer": preprocess_log_visit_referer_query(GCP_PROJECT, BIGQUERY_RAW_DATASET),
    "_config": preprocess_log_visit_config_query(GCP_PROJECT, BIGQUERY_RAW_DATASET),
    "_location": preprocess_log_visit_location_query(
        GCP_PROJECT, BIGQUERY_RAW_DATASET, ENV
    ),
    "_campaign": preprocess_log_visit_campaign_query(GCP_PROJECT, BIGQUERY_RAW_DATASET),
    "_custom_var": preprocess_log_visit_custom_var_query(
        GCP_PROJECT, BIGQUERY_RAW_DATASET
    ),
}

## -------------
## MATOMO_VISITS
## -------------

normalize_matomo_visits_tasks = []
matomo_visits_to_analytics = []

for column_group in normalize_log_visit_queries:
    normalize_matomo_visits_task = BigQueryOperator(
        task_id=f"normalize_matomo_visits{column_group}",
        sql=normalize_log_visit_queries[column_group],
        destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.matomo_visits{column_group}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        dag=dag,
    )

    normalize_matomo_visits_tasks.append(normalize_matomo_visits_task)

    matomo_visits_table_to_analytics = BigQueryOperator(
        task_id=f"matomo_visits{column_group}_to_analytics",
        sql=copy_matomo_visits_to_analytics(
            GCP_PROJECT, BIGQUERY_CLEAN_DATASET, column_group
        ),
        destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.matomo_visits{column_group}",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
        dag=dag,
    )

    normalize_matomo_visits_task >> matomo_visits_table_to_analytics

    matomo_visits_to_analytics.append(matomo_visits_table_to_analytics)

matomo_visits_end = DummyOperator(task_id="matomo_visits_end", dag=dag)
end_import >> matomo_visits_start >> normalize_matomo_visits_tasks
matomo_visits_to_analytics >> matomo_visits_end >> end_preprocess

## -------------
## LEGACY PREPROCESS
## -------------

preprocess_log_action_query = f"""
WITH filtered AS (
    SELECT *,
    REGEXP_EXTRACT(name, r"Module name: (.*) -") as module_name,
    REGEXP_EXTRACT(name, r"Number of tiles: ([0-9]*)") as number_tiles,
    REGEXP_EXTRACT(name, r"Offer id: ([A-Z0-9]*)") as offer_id,
    REGEXP_EXTRACT(name, r"details\/([A-Z0-9]{{4,5}})[^a-zA-Z0-9]") as offer_id_from_url,
    FROM {BIGQUERY_RAW_DATASET}.log_action
    WHERE type not in (1, 2, 3, 8)
    OR (type = 1 AND name LIKE "%.passculture.beta.gouv.fr/%")
),
dehumanized AS (
    SELECT
        *,
        IF( offer_id is not null,
            {BIGQUERY_RAW_DATASET}.dehumanize_id(offer_id), null) AS dehumanize_offer_id,
        IF( offer_id_from_url is not null,
            {BIGQUERY_RAW_DATASET}.dehumanize_id(offer_id_from_url), null) AS dehumanize_offer_id_from_url
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
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.log_action_preprocessed",
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
FROM {BIGQUERY_RAW_DATASET}.log_link_visit_action
"""

preprocess_log_link_visit_action_task = BigQueryOperator(
    task_id="preprocess_log_link_visit_action",
    sql=preprocess_log_link_visit_action_query,
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

copy_log_conversion_task = BigQueryOperator(
    task_id="copy_log_conversion",
    sql=f"SELECT * from {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.log_conversion",
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.log_conversion",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

copy_goal_task = BigQueryOperator(
    task_id="copy_goal",
    sql=f"SELECT * from {GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.goal",
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.goal",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

filter_log_link_visit_action_query = f"""
DELETE
FROM {BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed as llvap
WHERE llvap.idlink_va IN
(
    SELECT idlink_va
    FROM {BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed as llvap
    LEFT OUTER JOIN {BIGQUERY_CLEAN_DATASET}.log_action_preprocessed as lap1
        ON lap1.raw_data.idaction = llvap.idaction_url
    LEFT OUTER JOIN {BIGQUERY_CLEAN_DATASET}.log_action_preprocessed as lap2
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
DELETE FROM {BIGQUERY_CLEAN_DATASET}.log_action_preprocessed as lap
WHERE lap.raw_data.idaction IN (
    SELECT lap.raw_data.idaction
    FROM {BIGQUERY_CLEAN_DATASET}.log_action_preprocessed as lap
    LEFT OUTER JOIN {BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed as llvap1
        ON lap.raw_data.idaction = llvap1.idaction_url
    LEFT OUTER JOIN {BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed as llvap2
        ON lap.raw_data.idaction = llvap2.idaction_name
    LEFT OUTER JOIN {BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed as llvap3
        ON lap.raw_data.idaction = llvap3.idaction_event_action
    LEFT OUTER JOIN {BIGQUERY_CLEAN_DATASET}.log_link_visit_action_preprocessed as llvap4
        ON lap.raw_data.idaction = llvap4.idaction_event_category
    WHERE
        llvap1.idaction_url is null
    AND
        llvap2.idaction_name is null
    AND
        llvap3.idaction_event_action is null
    AND
        llvap4.idaction_event_category is null
)
"""

filter_log_action_task = BigQueryOperator(
    task_id="filter_log_action",
    sql=filter_log_action_query,
    use_legacy_sql=False,
    dag=dag,
)

# TASKS FOR TRANSFORMING MATOMO
transform_matomo_events = BigQueryOperator(
    task_id="transform_matomo_events",
    sql=transform_matomo_events_query(GCP_PROJECT, BIGQUERY_RAW_DATASET),
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.matomo_events",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

add_screen_view_matomo_events = BigQueryOperator(
    task_id="add_screen_view_matomo_events",
    sql=add_screen_view_matomo_events_query(GCP_PROJECT, BIGQUERY_RAW_DATASET),
    destination_dataset_table=f"{BIGQUERY_CLEAN_DATASET}.matomo_events",
    write_disposition="WRITE_APPEND",
    use_legacy_sql=False,
    dag=dag,
)

copy_events_to_analytics = BigQueryOperator(
    task_id="copy_events_to_analytics",
    sql=copy_events_to_analytics(GCP_PROJECT, BIGQUERY_CLEAN_DATASET),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.matomo_events",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

aggregate_matomo_offer_events = BigQueryOperator(
    task_id="aggregate_matomo_offer_events",
    sql=aggregate_matomo_offer_events(
        gcp_project=GCP_PROJECT,
        bigquery_raw_dataset=BIGQUERY_RAW_DATASET,
        bigquery_clean_dataset=BIGQUERY_CLEAN_DATASET,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.matomo_aggregated_offers",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)

aggregate_matomo_user_events = BigQueryOperator(
    task_id="aggregate_matomo_user_events",
    sql=aggregate_matomo_user_events(
        gcp_project=GCP_PROJECT,
        bigquery_clean_dataset=BIGQUERY_CLEAN_DATASET,
    ),
    destination_dataset_table=f"{BIGQUERY_ANALYTICS_DATASET}.matomo_aggregated_users",
    write_disposition="WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag,
)


(
    end_import
    >> [
        preprocess_log_action_task,
        preprocess_log_link_visit_action_task,
        copy_log_conversion_task,
        copy_goal_task,
    ]
    >> end_preprocess
)

(
    end_import
    >> transform_matomo_events
    >> add_screen_view_matomo_events
    >> copy_events_to_analytics
    >> aggregate_matomo_offer_events
    >> aggregate_matomo_user_events
    >> end_preprocess
)

end_dag = DummyOperator(task_id="end_dag", dag=dag)

end_preprocess >> filter_log_action_task >> filter_log_link_visit_action_task >> end_dag
