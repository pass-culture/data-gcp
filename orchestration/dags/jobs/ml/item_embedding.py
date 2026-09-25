from datetime import datetime, timedelta
from itertools import chain

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.alerts import SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN
from common.alerts.ml_training import create_item_embedding_slack_block
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_INPUT_DATASET,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCE_ZONES,
    GCP_PROJECT_ID,
    GCP_REGION,
    INSTANCES_TYPES,
    ML_BUCKET_TEMP,
)
from common.hooks.slack import SlackHook
from common.operators.bigquery import BigQueryInsertJobOperator
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from pydantic import BaseModel

from jobs.crons import SCHEDULE_DICT

###########################################################################
## GCS CONSTANTS
# Each run lays out per-vector, per-step subfolders under this prefix:
#   {GCS_FOLDER_PATH}/{vector}/{input,prompts,embeddings}/
GCS_FOLDER_PATH = f"item_embedding_{ENV_SHORT_NAME}/{{{{ ts_nodash }}}}"
INPUT_SUBFOLDER = "input"
PROMPTS_SUBFOLDER = "prompts"
EMBEDDINGS_SUBFOLDER = "embeddings"


class VectorPipeline(BaseModel):
    name: str  # embedding vector name
    input_table: str  # dataset.table (dbt input model)
    output_table: str  # dataset.table (this DAG's per-vector staging output)


# List of all the vectors this DAG can run. Add a vector by creating its dbt input model + a
# configs/<name>.yaml in the item_embeddings job + an entry here (all keyed by ``name``).
AVAILABLE_VECTORS = [
    VectorPipeline(
        name="all_items_metadata",
        input_table=f"{BIGQUERY_ML_INPUT_DATASET}.all_items_metadata_to_embed",
        output_table=f"{BIGQUERY_TMP_DATASET}.all_items_metadata_tmp",
    ),
    VectorPipeline(
        name="movies_metadata",
        input_table=f"{BIGQUERY_ML_INPUT_DATASET}.movies_metadata_to_embed",
        output_table=f"{BIGQUERY_TMP_DATASET}.movies_metadata_tmp",
    ),
    VectorPipeline(
        name="books_metadata",
        input_table=f"{BIGQUERY_ML_INPUT_DATASET}.books_metadata_to_embed",
        output_table=f"{BIGQUERY_TMP_DATASET}.books_metadata_tmp",
    ),
    VectorPipeline(
        name="all_items_offer_names",
        input_table=f"{BIGQUERY_ML_INPUT_DATASET}.all_items_metadata_to_embed",  # uses the same input as all_items_metadata
        output_table=f"{BIGQUERY_TMP_DATASET}.all_items_offer_names_tmp",
    ),
]
VECTOR_NAMES = [vector.name for vector in AVAILABLE_VECTORS]

## DAG CONFIG
DAG_NAME = "item_embedding"
BASE_DIR = "data-gcp/jobs/ml_jobs/item_embedding"
INSTANCE_NAME = "item-embedding"
GCE_ZONE_TEMPLATE = "{{ params.gce_zone }}"
INSTANCE_TYPE = {
    "dev": "n1-standard-8",
    "stg": "n1-standard-16",
    "prod": "n1-standard-16",
}[ENV_SHORT_NAME]

DEFAULT_ARGS = {
    "start_date": datetime(2025, 12, 1),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _step_command(
    module: str, vector_name: str, input_subfolder: str, output_subfolder: str
) -> str:
    """Build an SSH command for one pipeline step (preprocess/build_prompts/
    embed) of one vector, reading ``input_subfolder`` and writing
    ``output_subfolder`` under that vector's GCS prefix.

    Only vectors kept by the plan reach this step -- the per-vector
    ``check_in_plan`` ShortCircuit skips a dropped vector's whole subchain -- so
    no in-command guard is needed.
    """
    base = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{vector_name}"
    return (
        f"uv run python -m {module} "
        f"--config-file-name {vector_name} "
        f"--input-parquets-folder-path {base}/{input_subfolder} "
        f"--output-parquets-folder-path {base}/{output_subfolder}"
    )


def _export_input_query(vector: VectorPipeline) -> str:
    """EXPORT DATA query writing a vector's (optionally to_embed-filtered) input
    rows straight to GCS parquet -- no intermediate temp table needed.
    """
    uri = (
        f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}/{vector.name}/"
        f"{INPUT_SUBFOLDER}/item_metadata_*.parquet"
    )
    return f"""
        EXPORT DATA OPTIONS(
          uri='{uri}',
          format='PARQUET',
          overwrite=true
        ) AS
        SELECT * FROM `{GCP_PROJECT_ID}.{vector.input_table}`
        {{% if not params.embed_all %}}WHERE to_embed{{% endif %}}
    """


def _plan_vectors_to_embed(**context) -> list[str]:
    """Resolve, once and upstream of the per-vector fan-out, which vectors this
    run should actually process: those that are *selected* and have rows to
    embed in their own input table. embed_all forces every selected vector.

    A single batched query (one ``EXISTS`` probe per selected table, UNION ALL'd)
    replaces per-vector count queries. Backing a ShortCircuitOperator, the
    returned list is both the plan (read back by each vector's check via XCom)
    and the run-level gate -- an empty list is falsy, so the whole run skips.
    """
    params = context["params"]
    selected = [v for v in AVAILABLE_VECTORS if v.name in params["vectors"]]
    if params["embed_all"]:
        return [v.name for v in selected]
    if not selected:
        return []
    bq_hook = BigQueryHook(location=GCP_REGION, use_legacy_sql=False)
    query = "\nUNION ALL\n".join(
        f"(SELECT '{v.name}' AS vector "
        f"FROM `{GCP_PROJECT_ID}.{v.input_table}` WHERE to_embed LIMIT 1)"
        for v in selected
    )
    return [row[0] for row in bq_hook.get_records(query)]


def _vector_in_plan(vector_name: str, **context) -> bool:
    """ShortCircuit gate: run this vector only if the upstream plan kept it."""
    plan = context["ti"].xcom_pull(task_ids="plan_vectors_to_embed") or []
    return vector_name in plan


def _send_slack_notif_success(**context) -> None:
    plan = context["ti"].xcom_pull(task_ids="plan_vectors_to_embed") or []
    if not plan:
        summary = "Aucun vecteur embeddé."
    else:
        vectors_by_name = {v.name: v for v in AVAILABLE_VECTORS}
        bq_hook = BigQueryHook(location=GCP_REGION, use_legacy_sql=False)
        query = "\nUNION ALL\n".join(
            f"(SELECT '{name}' AS vector, COUNT(*) AS nb_rows "
            f"FROM `{GCP_PROJECT_ID}.{vectors_by_name[name].output_table}`)"
            for name in plan
        )
        counts = dict(bq_hook.get_records(query))
        summary = "\n".join(f"• *{name}*: {counts.get(name, 0)} items" for name in plan)
    block = create_item_embedding_slack_block(summary, ENV_SHORT_NAME)
    SlackHook(SLACK_ALERT_CHANNEL_WEBHOOK_TOKEN).send_message(None, block)


############################################################################
DAG_DOC = """
    ### Item embedding DAG

    Per vector (chosen via *vectors*), the DAG runs a GCS-staged pipeline:
    export input from its dbt table → prepare (preprocess + build prompts) →
    embed → load into its own BigQuery staging table (`<name>_metadata_tmp`).
    A later dbt model merges the staging tables.

    #### Parameters:
    * *embed_all* : whether to embed all items or only the ones that need embedding (to_embed = true in the input tables).
    * *vectors* : which embedding vectors to run (defaults to all). Each selected vector reads its own dbt input table and writes its own output table.
    * *instance_type* : GCE instance type. For L4 GPUs pick a compatible g2 machine (see hint).
    * *instance_name* : GCE instance name.
    * *gpu_type* : full catalogue → 4×L4 in europe-west1-c; incremental → 4×T4 (more widely available).
    * *gpu_count* : number of GPUs (must match the machine type).
    * *gce_zone* : only europe-west1-c/b have L4; europe-west1-d has T4. Stockouts are frequent.
    * *provisioning_model* : STANDARD (fails on stockout) or FLEX_START (DWS queues the request, held up to *request_valid_for_duration*, max 2h).
    * *max_run_duration* / *request_valid_for_duration* : FLEX_START only.
    * *reservation_name* : consume a specific reservation (requires provisioning_model=STANDARD; instance/gpu/zone must match it exactly).

    ⚠️ The per-vector embeds run **sequentially on one shared VM** (chained to
    avoid GPU contention). For *embed_all*, trigger the DAG **one vector at a
    time** (set *vectors* to a single entry) so each full-catalogue vector gets
    its own VM/sizing instead of queuing behind the others.

    *Hint:* L4 count per g2 machine: g2-standard-4/8/12/16/32 → 1, -24 → 2,
    -48 → 4, -96 → 8. Frequent L4 stockouts in europe-west1-b; try -c.
"""

with DAG(
    DAG_NAME,
    default_args=DEFAULT_ARGS,
    description="Embed items metadata",
    doc_md=DAG_DOC,
    schedule=SCHEDULE_DICT[DAG_NAME][ENV_SHORT_NAME],
    catchup=False,
    dagrun_timeout=timedelta(hours=30),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "embed_all": Param(
            default=False,
            type="boolean",
            description="Whether to embed all items or only the ones that need embedding (to_embed = true). See DAG docs for VM setup.",
        ),
        "vectors": Param(
            default=VECTOR_NAMES,
            type="array",
            items={"type": "string", "enum": VECTOR_NAMES},
            examples=VECTOR_NAMES,
            description="Embedding vectors to run (subset of AVAILABLE_VECTORS). "
            "Defaults to all.",
        ),
        "instance_type": Param(
            default=INSTANCE_TYPE,
            type="string",
            enum=list(chain(*INSTANCES_TYPES["cpu"].values())),
            description="GCE instance type",
        ),
        "instance_name": Param(
            default=INSTANCE_NAME,
            type="string",
            description="GCE instance name",
        ),
        "gpu_type": Param(
            default="nvidia-tesla-t4",
            enum=INSTANCES_TYPES["gpu"]["name"],
        ),
        "gpu_count": Param(
            default=0 if ENV_SHORT_NAME == "dev" else 1,
            enum=INSTANCES_TYPES["gpu"]["count"],
            description="Number of GPUs (only for GPU instance types; must match the machine type).",
        ),
        "gce_zone": Param(default="europe-west1-c", enum=GCE_ZONES),
        "provisioning_model": Param(
            default="STANDARD" if ENV_SHORT_NAME == "dev" else "FLEX_START",
            enum=["STANDARD", "FLEX_START"],
            description="""VM provisioning model. STANDARD requests capacity
                        immediately (fails on stockout). FLEX_START uses Dynamic
                        Workload Scheduler (DWS) to queue the GPU request until
                        capacity is available (queue held for up to
                        request_valid_for_duration, max 2h).""",
        ),
        "max_run_duration": Param(
            default="30h",
            type="string",
            description="""(FLEX_START only) Max VM run duration before it is
                        auto-deleted. Accepts e.g. '12h', '1d2h', or seconds.
                        Max 7 days.""",
        ),
        "request_valid_for_duration": Param(
            default="2h",
            type="string",
            description="""(FLEX_START only) How long DWS holds the request in
                        queue while the VM is PENDING. Accepts e.g. '2h', '90m'.
                        Must be 0 or between 90s and 2h.""",
        ),
        "reservation_name": Param(
            default=None,
            type=["string", "null"],
            description="""Name of a specific Compute Engine reservation to
                        consume. When set, requires provisioning_model=STANDARD
                        and instance_type/gpu_type/gpu_count/gce_zone must match
                        the reservation exactly. Leave empty to not target one.""",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")

    # Resolve once which vectors will actually run (selected + non-empty). The
    # returned list is both the plan (read back by each vector's check) and the
    # gate: an empty list is falsy, so this ShortCircuit skips the whole run
    # (including the VM) when there's nothing to embed.
    plan_vectors = ShortCircuitOperator(
        task_id="plan_vectors_to_embed",
        python_callable=_plan_vectors_to_embed,
    )

    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        gpu_type="{{ params.gpu_type }}",
        gpu_count="{{ params.gpu_count }}",
        gce_zone=GCE_ZONE_TEMPLATE,
        labels={"job_type": "extra_long_ml", "dag_name": DAG_NAME},
        provisioning_model="{{ params.provisioning_model }}",
        max_run_duration="{{ params.max_run_duration }}",
        request_valid_for_duration="{{ params.request_valid_for_duration }}",
        reservation_name="{{ params.reservation_name }}",
        execution_timeout=timedelta(hours=3),
        retries=3,
    )

    install_dependencies = InstallDependenciesOperator(
        task_id="install_dependencies",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        branch="{{ params.branch }}",
        gce_zone=GCE_ZONE_TEMPLATE,
        python_version="3.11",
        retries=2,
    )

    start_mlflow_run = SSHGCEOperator(
        task_id="start_mlflow_run",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        gce_zone=GCE_ZONE_TEMPLATE,
        command=(
            "uv run python -m cli.mlflow_run "
            "--airflow-run-id {{ run_id }} "
            "{{ '--embed-all' if params.embed_all else '--no-embed-all' }}"
        ),
    )

    start >> plan_vectors >> gce_instance_start >> install_dependencies
    install_dependencies >> start_mlflow_run

    # Each vector's steps live in their own TaskGroup and run a fully sequential
    # subchain on the shared VM.
    previous_embed = None
    embed_tasks = []
    load_tasks = []

    for vector in AVAILABLE_VECTORS:
        with TaskGroup(group_id=vector.name):
            check_in_plan = ShortCircuitOperator(
                task_id=f"check_in_plan_{vector.name}",
                python_callable=_vector_in_plan,
                op_kwargs={"vector_name": vector.name},
                # Skip only this vector's own subchain, not the following vectors.
                ignore_downstream_trigger_rules=False,
                # Run even if the previous vector was skipped or failed.
                trigger_rule="all_done",
            )

            export_input = BigQueryInsertJobOperator(
                project_id=GCP_PROJECT_ID,
                task_id=f"export_input_{vector.name}",
                configuration={
                    "query": {
                        "query": _export_input_query(vector),
                        "useLegacySql": False,
                    }
                },
            )

            prepare = SSHGCEOperator(
                task_id=f"prepare_{vector.name}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                gce_zone=GCE_ZONE_TEMPLATE,
                command=_step_command(
                    "cli.prepare", vector.name, INPUT_SUBFOLDER, PROMPTS_SUBFOLDER
                ),
                deferrable=True,
            )

            embed = SSHGCEOperator(
                task_id=f"embed_{vector.name}",
                instance_name="{{ params.instance_name }}",
                base_dir=BASE_DIR,
                gce_zone=GCE_ZONE_TEMPLATE,
                command=_step_command(
                    "cli.embed", vector.name, PROMPTS_SUBFOLDER, EMBEDDINGS_SUBFOLDER
                ),
                deferrable=True,
            )

            load = GCSToBigQueryOperator(
                task_id=f"load_{vector.name}",
                project_id=GCP_PROJECT_ID,
                bucket=ML_BUCKET_TEMP,
                source_objects=[
                    f"{GCS_FOLDER_PATH}/{vector.name}/{EMBEDDINGS_SUBFOLDER}/*.parquet"
                ],
                destination_project_dataset_table=vector.output_table,
                source_format="PARQUET",
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
                # Collapse the Parquet LIST's into a native REPEATED FLOAT
                extra_config={"parquetOptions": {"enableListInference": True}},
            )

            check_in_plan >> export_input
            (
                [export_input, install_dependencies, start_mlflow_run]
                >> prepare
                >> embed
                >> load
            )

        # First vector starts once there's something to embed; each later vector
        # waits for the previous embed to free the GPU.
        if previous_embed is None:
            plan_vectors >> check_in_plan
        else:
            previous_embed >> check_in_plan

        previous_embed = embed
        embed_tasks.append(embed)
        load_tasks.append(load)

    gce_instance_delete = DeleteGCEOperator(
        task_id="gce_stop_task",
        instance_name="{{ params.instance_name }}",
        gce_zone=GCE_ZONE_TEMPLATE,
)

# Tell Airflow these are tied together
gce_instance_start.as_setup()
gce_instance_delete.as_teardown(setups=gce_instance_start)

    send_slack_notif_success = PythonOperator(
        task_id="send_slack_notif_success",
        python_callable=_send_slack_notif_success,
        trigger_rule="all_done",
    )

    stop = EmptyOperator(task_id="stop", trigger_rule="all_done")

    # The VM lives until every embed is done (they share it); loads read from GCS.
    embed_tasks >> gce_instance_delete
    ([gce_instance_delete, *load_tasks] >> send_slack_notif_success >> stop)
