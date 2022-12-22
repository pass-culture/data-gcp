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


class GCloudComputeSSHOperator(BashOperator):
    @apply_defaults
    def __init__(
        self,
        command: str,
        dag_config: dict = None,
        path_to_run_command: str = None,
        resource_id: str = GCE_TRAINING_INSTANCE,
        project_id: str = GCP_PROJECT_ID,
        zone: str = GCE_ZONE,
        *args,
        **kwargs,
    ):

        default_command = 'export PATH="/opt/conda/bin:/opt/conda/condabin:"+$PATH\n'
        if path_to_run_command:
            default_command += f"cd {path_to_run_command}\n"
        if dag_config:
            default_command += "\n".join(
                [f"export {key}={value}" for key, value in dag_config.items()]
            )

        super(GCloudComputeSSHOperator, self).__init__(
            bash_command=f"gcloud compute ssh {resource_id} "
            f"--zone {zone} "
            f"--project {project_id} "
            f"--command $'{default_command}\n{command}'",
            *args,
            **kwargs,
        )
