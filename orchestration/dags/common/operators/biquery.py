from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
)

from common.config import GCP_PROJECT_ID


def bigquery_job_task(dag, table, job_params, extra_params={}):
    return BigQueryInsertJobOperator(
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
