from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.decorators import apply_defaults

from common.config import (
    GCP_PROJECT,
    GCP_PROJECT_ID,
    GCE_ZONE,
    GCE_TRAINING_INSTANCE,
)


def bigquery_job_task(dag, table, job_params):
    return BigQueryInsertJobOperator(
        task_id=table,
        configuration={
            "query": {
                "query": "{% include '" + job_params["sql"] + "' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": job_params["destination_dataset"],
                    "tableId": job_params["destination_table"],
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "timePartitioning": job_params.get("time_partitioning", None),
                "clustering": job_params.get("clustering_fields", None),
            },
        },
        trigger_rule=job_params.get("trigger_rule", "all_success"),
        params=dict(job_params.get("params", {})),
        dag=dag,
    )
