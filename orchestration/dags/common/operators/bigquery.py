from common.config import (
    APPLICATIVE_EXTERNAL_CONNECTION_ID,
    GCP_PROJECT_ID,
)
from common.utils import one_line_query

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)


# TODO: rename table in task_id
def bigquery_job_task(dag, table, job_params, extra_params={}):
    return BigQueryInsertJobOperator(
        project_id=GCP_PROJECT_ID,
        task_id=table,
        configuration={
            "query": {
                "query": "{% include '" + job_params["sql"] + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": job_params.get("destination_project", GCP_PROJECT_ID),
                    "datasetId": job_params["destination_dataset"],
                    "tableId": job_params["destination_table"],
                },
                "writeDisposition": job_params.get(
                    "write_disposition", "WRITE_TRUNCATE"
                ),
                "timePartitioning": job_params.get("time_partitioning", None),
                "clustering": job_params.get("clustering_fields", None),
                "schemaUpdateOptions": job_params.get("schemaUpdateOptions", None),
            }
        },
        trigger_rule=job_params.get("trigger_rule", "all_success"),
        params=dict(job_params.get("params", {}), **extra_params),
        dag=dag,
    )


def bigquery_view_task(dag, table, job_params, extra_params={}, exists_ok=True):
    return BigQueryCreateEmptyTableOperator(
        task_id=f"create_view_{table}",
        view={
            "query": "{% include '" + job_params["sql"] + "' %}",
            "useLegacySql": False,
        },
        dataset_id=job_params["destination_dataset"],
        table_id=job_params["destination_table"],
        trigger_rule=job_params.get("trigger_rule", "all_success"),
        params=dict(job_params.get("params", {}), **extra_params),
        dag=dag,
        exists_ok=exists_ok,
    )


def bigquery_federated_query_task(dag, task_id, job_params):
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={
            "query": {
                "query": f"""SELECT * FROM EXTERNAL_QUERY('{APPLICATIVE_EXTERNAL_CONNECTION_ID}', ''' {one_line_query(job_params['sql'])} ''')""",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": job_params["destination_dataset"],
                    "tableId": job_params["destination_table"],
                },
                "writeDisposition": job_params.get(
                    "write_disposition", "WRITE_TRUNCATE"
                ),
            }
        },
        params=dict(job_params.get("params", {})),
        dag=dag,
    )
