"""Book topics: unified pipeline (spec section 8), toggled between a weekly
full refit and the daily incremental run via the ``full_refit`` Param.

Three phases, each on its own GCE VM:

1. NLP VM (CPU): vocabulary cleaning/lemmatization.
   - full_refit: ``find-boilerplate`` -> ``select-delta`` (full) ->
     ``process`` -> ``update-registry``.
   - incremental: ``select-delta`` (against the previous
     ``topic_item_document`` snapshot) -> ``process`` -> ``update-registry``.
   Opens the shared MLflow run and captures its run_id.
2. Reduction+clustering VM (GPU -- UMAP needs a genuinely large GPU, kept on
   its own VM and separate from labelling's CPU-only work so GPU time isn't
   paid for during c-TF-IDF): per corpus (today: the 14 level-1 GTL nodes --
   split_level=1's documented default; see CORPORA below), largest first,
   chained sequentially on one shared VM to avoid GPU contention (mirrors
   item_embedding.py):
   - full_refit: ``reduction fetch-embeddings`` -> ``fit`` ->
     ``clustering fit`` (written straight to the model's own GCS path,
     since that IS this corpus's new authoritative state).
   - incremental: ``reduction fetch-embeddings`` (delta only) ->
     ``transform`` -> ``clustering predict`` against the last full refit's
     saved model (read directly over ``gs://``; polars speaks it natively,
     no local copy needed).
3. Labelling VM (CPU -- no GPU needed for sparse c-TF-IDF math): per corpus:
   - full_refit: ``labelling ctfidf`` -> ``export --full-refit`` (truncate
     + reload ``topic``/``item_topic``).
   - incremental: an inline ``merge_upsert`` of ``item_topic`` only (reusing
     the job's own ``src.common.bigquery``/``schemas`` code via
     ``uv run python -c``; ``labelling export``'s CLI assumes a paired
     ``topic`` table write, which doesn't apply here since no new topic
     words/labels are computed on a predict-only run).

All intermediate files are GCS paths (polars reads/writes ``gs://`` natively)
since each phase runs on its own, disposable VM.

Simplifications versus the spec, flagged for a follow-up:
- Always reprocesses the full vocabulary on a full refit (spec's "process
  if the processing version changed" skip-when-unchanged optimization isn't
  implemented).
- Runs every corpus unconditionally on an incremental run rather than
  skipping ones with no new items (spec's "for each corpus with new
  items") -- an empty delta should no-op through the pipeline, but this
  isn't verified end-to-end.
- CORPORA is a static list valid only for the documented default
  split_level=1; a different split_level needs its own corpus list (not
  built here -- see spec section 10 decision #5).
- Optional outlier reassignment (``labelling reassign``) isn't wired in
  here; ``reassign_outliers`` defaults to off anyway (spec section 10
  decision #4).
- A true first-ever run must use full_refit=True (no
  ``topic_item_document``/"latest" model pointer exist yet for an
  incremental run to read).
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    ML_BUCKET_TEMP,
)
from common.operators.bigquery import BigQueryInsertJobOperator
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule

from jobs.crons import SCHEDULE_DICT

DAG_NAME = "topics"
BASE_DIR = "data-gcp/jobs/ml_jobs/topics"
NLP_INSTANCE = "topics-nlp"
REDUCTION_INSTANCE = "topics-reduction"
LABELLING_INSTANCE = "topics-labelling"
MLFLOW_RUN_ID_FILE = "mlflow_run_id.txt"
BIGQUERY_ML_FEATURES_DATASET = f"ml_feat_{ENV_SHORT_NAME}"

# No existing convention for a persistent ML model bucket (only
# ML_BUCKET_TEMP does) -- mirrors the default already used by the job's own
# src/common/constants.py.
ML_BUCKET = os.environ.get("ML_BUCKET", f"data-bucket-ml-{ENV_SHORT_NAME}")

GCS_FOLDER_PATH = f"topics_{ENV_SHORT_NAME}/{{{{ ds_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
ITEMS_PARQUET = f"{STORAGE_BASE_PATH}/items_*.parquet"
PREVIOUS_DOCUMENTS_PARQUET = f"{STORAGE_BASE_PATH}/previous_documents_*.parquet"
PROCESSED_PARQUET = f"{STORAGE_BASE_PATH}/processed.parquet"
# Stable (not per-run): incremental runs reuse the last full refit's
# boilerplate segments rather than recomputing them.
BOILERPLATE_PATH = f"gs://{ML_BUCKET}/topics_{ENV_SHORT_NAME}/boilerplate.json"

# The 14 level-1 GTL nodes (spec section 3's table), largest first. Valid
# only for the documented default split_level=1 -- a different split_level
# needs its own corpus list (not built here; see spec section 10 decision #5).
CORPORA = [
    ("01000000", "Littérature"),
    ("09000000", "Sciences humaines & sociales"),
    ("06000000", "Arts et spectacles"),
    ("03000000", "Bandes dessinées / Comics / Mangas"),
    ("04000000", "Vie pratique & Loisirs"),
    ("02000000", "Jeunesse"),
    ("10000000", "Sciences & Techniques"),
    ("08000000", "Entreprise, économie & droit"),
    ("07000000", "Religion & Esotérisme"),
    ("05000000", "Tourisme & Voyages"),
    ("90000000", "Roman"),
    ("92000000", "Romance"),
    ("91000000", "Fantasy & Science-fiction"),
    ("13000000", "Dictionnaires / Encyclopédies / Documentation"),
]

ITEM_METADATA_EXPORT_QUERY = f"""
EXPORT DATA OPTIONS(
  uri='{ITEMS_PARQUET}',
  format='PARQUET',
  overwrite=true
) AS
SELECT
    item_id,
    offer_name,
    offer_description,
    gtl_type,
    titelive_gtl_id AS gtl_id,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
FROM `{GCP_PROJECT_ID}.ml_input_{ENV_SHORT_NAME}.item_metadata`
WHERE gtl_type = 'BOOK' AND titelive_gtl_id IS NOT NULL
"""

# A harmless dummy query on a full refit: no previous snapshot is needed,
# and topic_item_document may not exist yet on a true first-ever run.
PREVIOUS_DOCUMENTS_EXPORT_QUERY = f"""
{{% if not params.full_refit %}}
EXPORT DATA OPTIONS(
  uri='{PREVIOUS_DOCUMENTS_PARQUET}',
  format='PARQUET',
  overwrite=true
) AS
SELECT item_id, description_hash
FROM `{GCP_PROJECT_ID}.{BIGQUERY_ML_FEATURES_DATASET}.topic_item_document`
{{% else %}}
SELECT 1
{{% endif %}}
"""


def model_path(split_level: str, corpus_id: str, run_id: str) -> str:
    """Mirrors ``src.common.gcs.model_path`` (can't import job code from a
    DAG file -- separate deployable units).
    """
    return (
        f"gs://{ML_BUCKET}/topics_{ENV_SHORT_NAME}/models/"
        f"level{split_level}/{corpus_id}/{run_id}/"
    )


def corpus_gtl_ids(corpus_id: str, **context) -> list[str]:
    """Every GTL id whose items belong to ``corpus_id`` at this run's split
    level (spec sections 4.4/5): same-length prefix, right-padded with "00".
    """
    split_level = int(context["params"]["split_level"])
    prefix_length = 2 * split_level
    bq_hook = BigQueryHook(location="EU", use_legacy_sql=False)
    query = f"""
        select distinct titelive_gtl_id
        from `{GCP_PROJECT_ID}.ml_input_{ENV_SHORT_NAME}.item_metadata`
        where gtl_type = 'BOOK'
          and titelive_gtl_id is not null
          and rpad(left(titelive_gtl_id, {prefix_length}), 8, '0') = '{corpus_id}'
    """
    return [row[0] for row in bq_hook.get_records(query)]


def previous_processing_version(**context) -> str | None:
    """The most common ``processing_version`` currently in
    ``topic_item_document`` -- ``None`` on a full refit, or on a first
    incremental run (table doesn't exist yet or is empty).
    """
    if context["params"]["full_refit"]:
        return None
    bq_hook = BigQueryHook(location="EU", use_legacy_sql=False)
    query = f"""
        select processing_version, count(*) as n
        from `{GCP_PROJECT_ID}.{BIGQUERY_ML_FEATURES_DATASET}.topic_item_document`
        group by processing_version
        order by n desc
        limit 1
    """
    try:
        rows = bq_hook.get_records(query)
    except Exception:  # noqa: BLE001 -- table may not exist yet on a first run
        return None
    return rows[0][0] if rows else None


def latest_model_run_id(split_level: str, corpus_id: str, **context) -> str:
    """The run_id of the model currently marked "latest" for a corpus (spec
    sections 7.2/7.3's "latest" pointer). Mirrors
    ``src.common.gcs.read_latest_run_id`` (can't import job code here).
    Returns "" rather than raising when unset (e.g. unused on a full refit,
    or a genuine first run).
    """
    from google.cloud import storage

    blob_path = f"topics_{ENV_SHORT_NAME}/models/level{split_level}/{corpus_id}/latest"
    blob = storage.Client().bucket(ML_BUCKET).blob(blob_path)
    if not blob.exists():
        return ""
    return blob.download_as_text().strip()


def _corpus_run_paths(corpus_id: str, new_run_id: str) -> tuple[str, str, str]:
    """``(resolved_run_id, corpus_model_path, clusters_path)`` Jinja-templated
    strings for one corpus. Called identically from both the
    reduction/clustering and labelling loops (which build separate
    TaskGroups, so there's no shared Python object to reuse) -- factored
    here once so the two loops can't drift out of sync.
    """
    latest_run_id = (
        "{{ ti.xcom_pull(task_ids="
        f"'reduction_clustering_phase.corpus_{corpus_id}."
        f"fetch_latest_run_id_{corpus_id}') }}"
    )
    resolved_run_id = (
        "{% if params.full_refit %}"
        + new_run_id
        + "{% else %}"
        + latest_run_id
        + "{% endif %}"
    )
    corpus_model_path = model_path(
        "{{ params.split_level }}", corpus_id, resolved_run_id
    )
    clusters_path = (
        "{% if params.full_refit %}"
        + f"{corpus_model_path}clusters.parquet"
        + "{% else %}"
        + f"{STORAGE_BASE_PATH}/clusters_{corpus_id}.parquet"
        + "{% endif %}"
    )
    return resolved_run_id, corpus_model_path, clusters_path


default_args = {
    "start_date": datetime(2026, 11, 10),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 2,
}

with DAG(
    DAG_NAME,
    default_args=default_args,
    description=(
        "Book topics pipeline: daily incremental, or a full refit via the "
        "full_refit param"
    ),
    schedule=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "full_refit": Param(
            default=False,
            type="boolean",
            description="True for the weekly full refit (refits UMAP/HDBSCAN "
            "and recomputes topic words/labels); False for the daily "
            "incremental run (predicts against the last full refit's model).",
        ),
        "nlp_instance_type": Param(default="n2-highcpu-32", type="string"),
        "reduction_instance_type": Param(default="n1-standard-16", type="string"),
        "reduction_gpu_type": Param(default="nvidia-tesla-t4", type="string"),
        "reduction_gpu_count": Param(default=1, type="integer"),
        "labelling_instance_type": Param(default="n2-highcpu-16", type="string"),
        "split_level": Param(default=1, type="integer"),
        "processing_version": Param(default="v1", type="string"),
        "config_file_name": Param(default="default", type="string"),
    },
) as dag:
    export_items = BigQueryInsertJobOperator(
        task_id="export_items_to_gcs",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {"query": ITEM_METADATA_EXPORT_QUERY, "useLegacySql": False}
        },
    )
    export_previous_documents = BigQueryInsertJobOperator(
        task_id="export_previous_documents_to_gcs",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {"query": PREVIOUS_DOCUMENTS_EXPORT_QUERY, "useLegacySql": False}
        },
    )
    fetch_previous_processing_version = PythonOperator(
        task_id="fetch_previous_processing_version",
        python_callable=previous_processing_version,
    )
    previous_version_xcom = (
        "{{ ti.xcom_pull(task_ids='fetch_previous_processing_version') }}"
    )

    with TaskGroup("nlp_phase") as nlp_phase:
        nlp_start = StartGCEOperator(
            task_id="nlp_start_task",
            instance_name=NLP_INSTANCE,
            instance_type="{{ params.nlp_instance_type }}",
            preemptible=False,
            labels={"job_type": "long_ml", "dag_name": DAG_NAME},
        )
        nlp_install = InstallDependenciesOperator(
            task_id="nlp_install_dependencies",
            instance_name=NLP_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.13",
            base_dir=BASE_DIR,
            extras=["vocabulary"],
            retries=2,
        )

        start_mlflow_run = SSHGCEOperator(
            task_id="start_mlflow_run",
            instance_name=NLP_INSTANCE,
            base_dir=BASE_DIR,
            command=(
                "uv run python main.py common start "
                "--config-file-name {{ params.config_file_name }} "
                "--split-level {{ params.split_level }} "
                "--airflow-run-id {{ run_id }}\n"
                f"cat {MLFLOW_RUN_ID_FILE}"
            ),
        )

        find_boilerplate = SSHGCEOperator(
            task_id="find_boilerplate",
            instance_name=NLP_INSTANCE,
            base_dir=BASE_DIR,
            command=(
                "{% if params.full_refit %}"
                "uv run python main.py vocabulary find-boilerplate "
                f"--input-file-path {ITEMS_PARQUET} "
                f"--output-file-path {BOILERPLATE_PATH} "
                "--config-file-name {{ params.config_file_name }}"
                "{% else %}"
                "echo 'Incremental run: reusing the last full refit's "
                "boilerplate segments.'"
                "{% endif %}"
            ),
        )

        select_delta = SSHGCEOperator(
            task_id="select_delta",
            instance_name=NLP_INSTANCE,
            base_dir=BASE_DIR,
            command=(
                "uv run python main.py vocabulary select-delta "
                f"--input-file-path {ITEMS_PARQUET} "
                f"--boilerplate-file-path {BOILERPLATE_PATH} "
                "--output-file-path delta.parquet "
                "--processing-version {{ params.processing_version }} "
                "{% if not params.full_refit %}"
                f"--previous-documents-file-path {PREVIOUS_DOCUMENTS_PARQUET} "
                f"--previous-processing-version {previous_version_xcom} "
                "{% endif %}"
                "--config-file-name {{ params.config_file_name }}"
            ),
        )

        process = SSHGCEOperator(
            task_id="process",
            instance_name=NLP_INSTANCE,
            base_dir=BASE_DIR,
            command=(
                "uv run python main.py vocabulary process "
                "--input-file-path delta.parquet "
                f"--output-file-path {PROCESSED_PARQUET} "
                "--processing-version {{ params.processing_version }} "
                "--config-file-name {{ params.config_file_name }}"
            ),
        )

        update_registry = SSHGCEOperator(
            task_id="update_registry",
            instance_name=NLP_INSTANCE,
            base_dir=BASE_DIR,
            command=(
                "uv run python main.py vocabulary update-registry "
                f"--input-file-path {PROCESSED_PARQUET} "
                "--config-file-name {{ params.config_file_name }}"
            ),
        )

        nlp_stop = DeleteGCEOperator(
            task_id="nlp_stop_task",
            instance_name=NLP_INSTANCE,
            trigger_rule="none_failed",
        )

        (
            nlp_start
            >> nlp_install
            >> start_mlflow_run
            >> find_boilerplate
            >> select_delta
            >> process
            >> update_registry
            >> nlp_stop
        )

    new_run_id = "{{ ti.xcom_pull(task_ids='nlp_phase.start_mlflow_run') }}"

    with TaskGroup("reduction_clustering_phase") as reduction_clustering_phase:
        reduction_start = StartGCEOperator(
            task_id="reduction_start_task",
            instance_name=REDUCTION_INSTANCE,
            instance_type="{{ params.reduction_instance_type }}",
            gpu_type="{{ params.reduction_gpu_type }}",
            gpu_count="{{ params.reduction_gpu_count }}",
            preemptible=False,
            labels={"job_type": "long_ml", "dag_name": DAG_NAME},
        )
        reduction_install = InstallDependenciesOperator(
            task_id="reduction_install_dependencies",
            instance_name=REDUCTION_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.13",
            base_dir=BASE_DIR,
            extras=["reduction", "clustering", "gpu"],
            retries=2,
        )

        previous_corpus_done = None
        for corpus_id, _corpus_label in CORPORA:
            resolved_run_id, corpus_model_path, clusters_path = _corpus_run_paths(
                corpus_id, new_run_id
            )
            latest_run_id = (
                "{{ ti.xcom_pull(task_ids="
                f"'reduction_clustering_phase.corpus_{corpus_id}."
                f"fetch_latest_run_id_{corpus_id}') }}"
            )

            with TaskGroup(group_id=f"corpus_{corpus_id}"):
                fetch_gtl_ids = PythonOperator(
                    task_id=f"fetch_gtl_ids_{corpus_id}",
                    python_callable=corpus_gtl_ids,
                    op_kwargs={"corpus_id": corpus_id},
                )
                fetch_latest_run_id = PythonOperator(
                    task_id=f"fetch_latest_run_id_{corpus_id}",
                    python_callable=latest_model_run_id,
                    op_kwargs={
                        "split_level": "{{ params.split_level }}",
                        "corpus_id": corpus_id,
                    },
                )
                gtl_ids_flags = (
                    "{% for gtl_id in ti.xcom_pull(task_ids="
                    f"'reduction_clustering_phase.corpus_{corpus_id}."
                    f"fetch_gtl_ids_{corpus_id}'"
                    ") %}--gtl-ids {{ gtl_id }} {% endfor %}"
                )
                embeddings_path = f"{STORAGE_BASE_PATH}/embeddings_{corpus_id}.parquet"
                vectors_path = f"{STORAGE_BASE_PATH}/vectors_{corpus_id}.parquet"

                fetch_embeddings = SSHGCEOperator(
                    task_id=f"fetch_embeddings_{corpus_id}",
                    instance_name=REDUCTION_INSTANCE,
                    base_dir=BASE_DIR,
                    command=(
                        "uv run python main.py reduction fetch-embeddings "
                        f"--documents-file-path {PROCESSED_PARQUET} "
                        f"--output-file-path {embeddings_path} "
                        f"{gtl_ids_flags}"
                        "--config-file-name {{ params.config_file_name }}"
                    ),
                )

                reduction_step = SSHGCEOperator(
                    task_id=f"reduction_{corpus_id}",
                    instance_name=REDUCTION_INSTANCE,
                    base_dir=BASE_DIR,
                    command=(
                        "{% if params.full_refit %}"
                        "uv run python main.py reduction fit "
                        f"--input-file-path {embeddings_path} "
                        f"--output-file-path {vectors_path} "
                        "--split-level {{ params.split_level }} "
                        f"--corpus-id {corpus_id} "
                        f"--run-id {new_run_id} "
                        "--config-file-name {{ params.config_file_name }}"
                        "{% else %}"
                        "uv run python main.py reduction transform "
                        f"--input-file-path {embeddings_path} "
                        f"--output-file-path {vectors_path} "
                        "--split-level {{ params.split_level }} "
                        f"--corpus-id {corpus_id} "
                        f"--run-id {latest_run_id}"
                        "{% endif %}"
                    ),
                )

                clustering_step = SSHGCEOperator(
                    task_id=f"clustering_{corpus_id}",
                    instance_name=REDUCTION_INSTANCE,
                    base_dir=BASE_DIR,
                    command=(
                        "{% if params.full_refit %}"
                        "uv run python main.py clustering fit "
                        f"--input-file-path {vectors_path} "
                        f"--output-file-path {clusters_path} "
                        "--split-level {{ params.split_level }} "
                        f"--corpus-id {corpus_id} "
                        f"--run-id {new_run_id} "
                        "--config-file-name {{ params.config_file_name }}"
                        "{% else %}"
                        "uv run python main.py clustering predict "
                        f"--input-file-path {vectors_path} "
                        f"--output-file-path {clusters_path} "
                        f"--cluster-mapping-file-path {corpus_model_path}clusters.parquet "
                        "--split-level {{ params.split_level }} "
                        f"--corpus-id {corpus_id} "
                        f"--run-id {latest_run_id}"
                        "{% endif %}"
                    ),
                )

                (
                    [fetch_gtl_ids, fetch_latest_run_id]
                    >> fetch_embeddings
                    >> reduction_step
                    >> clustering_step
                )

            if previous_corpus_done is not None:
                previous_corpus_done >> [fetch_gtl_ids, fetch_latest_run_id]
            reduction_install >> [fetch_gtl_ids, fetch_latest_run_id]
            previous_corpus_done = clustering_step

        reduction_stop = DeleteGCEOperator(
            task_id="reduction_stop_task",
            instance_name=REDUCTION_INSTANCE,
            trigger_rule="none_failed_min_one_success",
        )

        reduction_start >> reduction_install
        previous_corpus_done >> reduction_stop

    with TaskGroup("labelling_phase") as labelling_phase:
        labelling_start = StartGCEOperator(
            task_id="labelling_start_task",
            instance_name=LABELLING_INSTANCE,
            instance_type="{{ params.labelling_instance_type }}",
            preemptible=False,
            labels={"job_type": "long_ml", "dag_name": DAG_NAME},
        )
        labelling_install = InstallDependenciesOperator(
            task_id="labelling_install_dependencies",
            instance_name=LABELLING_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.13",
            base_dir=BASE_DIR,
            extras=["labelling"],
            retries=2,
        )

        previous_corpus_done = None
        for corpus_id, corpus_label in CORPORA:
            _resolved_run_id, _corpus_model_path, clusters_path = _corpus_run_paths(
                corpus_id, new_run_id
            )
            topics_path = f"{STORAGE_BASE_PATH}/topics_{corpus_id}.parquet"
            item_topic_path = f"{STORAGE_BASE_PATH}/item_topic_{corpus_id}.parquet"

            with TaskGroup(group_id=f"corpus_{corpus_id}"):
                labelling_step = SSHGCEOperator(
                    task_id=f"labelling_{corpus_id}",
                    instance_name=LABELLING_INSTANCE,
                    base_dir=BASE_DIR,
                    command=(
                        "{% if params.full_refit %}"
                        "uv run python main.py labelling ctfidf "
                        f"--documents-file-path {PROCESSED_PARQUET} "
                        f"--clusters-file-path {clusters_path} "
                        f"--topics-output-file-path {topics_path} "
                        f"--item-topic-output-file-path {item_topic_path} "
                        "--split-level {{ params.split_level }} "
                        f"--corpus-id {corpus_id} "
                        f"--corpus-label '{corpus_label}' "
                        "--config-file-name {{ params.config_file_name }}\n"
                        "uv run python main.py labelling export "
                        f"--topics-file-path {topics_path} "
                        f"--item-topic-file-path {item_topic_path} "
                        "--full-refit "
                        f"--run-id {new_run_id} "
                        "--config-file-name {{ params.config_file_name }}"
                        "{% else %}"
                        f"""uv run python -c "
import polars as pl
from src.common.bigquery import merge_upsert
from src.common.constants import BIGQUERY_ML_FEATURES_DATASET, MLFLOW_RUN_ID_COLUMN
from src.common.schemas import ITEM_TOPIC_SCHEMA

clusters = pl.read_parquet('{clusters_path}')
item_topic = clusters.select(
    'item_id', 'split_level', 'corpus_id', 'topic_uuid', 'is_outlier', 'assignment', 'run_id'
).with_columns(
    pl.col('is_outlier').alias('original_is_outlier'),
    pl.lit('{new_run_id}').alias(MLFLOW_RUN_ID_COLUMN),
)
merge_upsert(
    item_topic, BIGQUERY_ML_FEATURES_DATASET, 'item_topic',
    key_columns=['item_id'], schema=ITEM_TOPIC_SCHEMA,
)
\""""
                        "{% endif %}"
                    ),
                )

            if previous_corpus_done is not None:
                previous_corpus_done >> labelling_step
            labelling_install >> labelling_step
            previous_corpus_done = labelling_step

        labelling_stop = DeleteGCEOperator(
            task_id="labelling_stop_task",
            instance_name=LABELLING_INSTANCE,
            trigger_rule="none_failed_min_one_success",
        )

        labelling_start >> labelling_install
        previous_corpus_done >> labelling_stop

    (
        [export_items, export_previous_documents, fetch_previous_processing_version]
        >> nlp_phase
        >> reduction_clustering_phase
        >> labelling_phase
    )
