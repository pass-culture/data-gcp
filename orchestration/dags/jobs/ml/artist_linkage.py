import os
from datetime import datetime

from airflow import DAG
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup
from common import macros
from common.callback import on_failure_vm_callback
from common.config import (
    BIGQUERY_ML_LINKAGE_DATASET,
    BIGQUERY_ML_PREPROCESSING_DATASET,
    BIGQUERY_RAW_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    DATA_GCS_BUCKET_NAME,
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

DEFAULT_REGION = "europe-west1"
GCE_INSTANCE = f"artist-linkage-{ENV_SHORT_NAME}"
BASE_DIR = "data-gcp/jobs/ml_jobs/artist_linkage"
DAG_NAME = "artist_linkage"
DAG_VERSION = "2.0"
SUMMARIZE_BIOGRAPHY_OPTIONS = {
    "dev": "--number-of-biographies-to-summarize 20",
    "stg": "--number-of-biographies-to-summarize 100",
    "prod": "",
}

# GCS Paths / Filenames
GCS_FOLDER_PATH = f"artist_linkage_{ENV_SHORT_NAME}/{{{{ ds_nodash }}}}"
STORAGE_BASE_PATH = f"gs://{ML_BUCKET_TEMP}/{GCS_FOLDER_PATH}"
WIKIDATA_STORAGE_BASE_PATH = f"gs://{DATA_GCS_BUCKET_NAME}/dump_wikidata"
WIKIDATA_EXTRACTION_GCS_FILENAME = "wikidata_extraction.parquet"
PRODUCTS_TO_LINK_GCS_FILENAME = "products_to_link.parquet"
APPLICATIVE_ARTISTS_GCS_FILENAME = "applicative_database_artist.parquet"
APPLICATIVE_ARTIST_ALIAS_GCS_FILENAME = "applicative_database_artist_alias.parquet"
APPLICATIVE_PRODUCT_ARTIST_LINK_GCS_FILENAME = (
    "applicative_database_product_artist_link.parquet"
)
DELTA_ARTIST_ALIAS_GCS_FILENAME = "delta_artist_alias.parquet"
DELTA_PRODUCT_ARTIST_LINK_GCS_FILENAME = "delta_product_artist_link.parquet"
DELTA_ARTISTS_GCS_FILENAME = "delta_artist.parquet"
DELTA_ARTISTS_WITH_METADATA_GCS_FILENAME = "delta_artist_with_metadata.parquet"
DELTA_ARTISTS_WITH_WIKIPEDIA_PAGE_CONTENT_GCS_FILENAME = (
    "delta_artist_with_wikipedia_page_content.parquet"
)
DELTA_ARTISTS_WITH_BIOGRAPHY_GCS_FILENAME = "delta_artist_with_biography.parquet"


# BQ Tables
PRODUCT_TO_LINK_TABLE = "product_to_link"
TABLES_TO_IMPORT_TO_GCS = [
    {
        "dataset_id": BIGQUERY_RAW_DATASET,
        "table_id": "applicative_database_artist",
        "filename": APPLICATIVE_ARTISTS_GCS_FILENAME,
    },
    {
        "dataset_id": BIGQUERY_RAW_DATASET,
        "table_id": "applicative_database_artist_alias",
        "filename": APPLICATIVE_ARTIST_ALIAS_GCS_FILENAME,
    },
    {
        "dataset_id": BIGQUERY_RAW_DATASET,
        "table_id": "applicative_database_product_artist_link",
        "filename": APPLICATIVE_PRODUCT_ARTIST_LINK_GCS_FILENAME,
    },
    {
        "dataset_id": BIGQUERY_ML_LINKAGE_DATASET,
        "table_id": PRODUCT_TO_LINK_TABLE,
        "filename": PRODUCTS_TO_LINK_GCS_FILENAME,
    },
]
GCS_TO_DELTA_TABLES = [
    {
        "dataset_id": BIGQUERY_ML_PREPROCESSING_DATASET,
        "table_id": "delta_artist",
        "filename": DELTA_ARTISTS_WITH_BIOGRAPHY_GCS_FILENAME,
    },
    {
        "dataset_id": BIGQUERY_ML_PREPROCESSING_DATASET,
        "table_id": "delta_artist_alias",
        "filename": DELTA_ARTIST_ALIAS_GCS_FILENAME,
    },
    {
        "dataset_id": BIGQUERY_ML_PREPROCESSING_DATASET,
        "table_id": "delta_product_artist_link",
        "filename": DELTA_PRODUCT_ARTIST_LINK_GCS_FILENAME,
    },
]

# Flow names
INCREMENTAL_FLOW = "incremental_flow"
REFRESH_METADATA_FLOW = "refresh_metadata_flow"
SKIP_SUMMERIZATION_WITH_LLM_FLOW = "skip_summerization_with_llm_flow"
SUMMERIZATION_WITH_LLM_FLOW = "summerization_with_llm_flow"

# DAG Documentation
DAG_MD_DOC = """
# Artist Linkage DAG

This DAG links products to artists using clustering algorithms and enriches artist data with Wikidata metadata.

## Two Execution Flows

The DAG supports two different execution modes, controlled by the `linkage_mode` parameter:

### 1. Incremental Flow (`incremental`)
Updates the existing artist database with new products only.

**Steps:**
- Links new products to existing artists or creates new artist entries
- Enriches delta artist data with Wikimedia Commons licenses
- Loads delta tables (delta_artist, delta_artist_alias, delta_product_artist_link) into BigQuery

**Use case:** Regular updates to link newly added products to the artist database.

### 2. Refresh Metadata Flow (`metadata_refresh`)
Refreshes Wikidata metadata for existing artists without relinking products.

**Steps:**
- Retrieves updated Wikidata information for existing artists
- Enriches delta artist data with updated Wikimedia Commons licenses
- Loads delta tables with refreshed metadata into BigQuery

**Use case:** Periodic updates to keep artist metadata (descriptions, images, etc.) up to date without reprocessing product links.
"""


def _choose_linkage(**context):
    if context["params"]["linkage_mode"] == "metadata_refresh":
        return REFRESH_METADATA_FLOW
    return INCREMENTAL_FLOW


def _skip_llm_summerization(**context):
    if context["params"]["skip_llm_summerization"]:
        return SKIP_SUMMERIZATION_WITH_LLM_FLOW
    return SUMMERIZATION_WITH_LLM_FLOW


default_args = {
    "start_date": datetime(2024, 7, 16),
    "on_failure_callback": on_failure_vm_callback,
    "retries": 5,
}
with DAG(
    DAG_NAME,
    default_args=default_args,
    description="Link artists via clustering",
    schedule_interval=get_airflow_schedule(SCHEDULE_DICT[DAG_NAME]),
    catchup=False,
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    doc_md=DAG_MD_DOC,
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default="n1-standard-2" if ENV_SHORT_NAME == "dev" else "n1-standard-8",
            type="string",
        ),
        "linkage_mode": Param(
            default="incremental",
            enum=["incremental", "metadata_refresh"],
            type="string",
        ),
        "skip_llm_summerization": Param(default=False, type="bool"),
    },
) as dag:
    with TaskGroup("dag_init") as dag_init:
        logging_task = PythonOperator(
            task_id="logging_task",
            python_callable=lambda: print(
                f"Task executed for branch : {dag.params.get('branch')} and instance : {dag.params.get('instance_type')} on env : {ENV_SHORT_NAME}"
            ),
            dag=dag,
        )

    with TaskGroup("vm_init") as vm_init:
        gce_instance_start = StartGCEOperator(
            task_id="gce_start_task",
            instance_name=GCE_INSTANCE,
            instance_type="{{ params.instance_type }}",
            preemptible=False,
            labels={"job_type": "ml", "dag_name": DAG_NAME},
        )
        fetch_install_code = InstallDependenciesOperator(
            task_id="fetch_install_code",
            instance_name=GCE_INSTANCE,
            branch="{{ params.branch }}",
            python_version="3.10",
            base_dir=BASE_DIR,
            retries=2,
        )
        gce_instance_start >> fetch_install_code

    join_before_stop = EmptyOperator(
        task_id="join_before_stop",
        trigger_rule="none_failed_min_one_success",
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name=GCE_INSTANCE, trigger_rule="none_failed"
    )

    choose_linkage = BranchPythonOperator(
        task_id="choose_path",
        python_callable=_choose_linkage,
        provide_context=True,
        dag=dag,
    )
    choose_llm_summerization = BranchPythonOperator(
        task_id="choose_llm_summerization",
        python_callable=_skip_llm_summerization,
        provide_context=True,
        dag=dag,
    )
    incremental_flow = EmptyOperator(task_id=INCREMENTAL_FLOW)
    refresh_metadata_flow = EmptyOperator(task_id=REFRESH_METADATA_FLOW)
    summerization_with_llm_flow = EmptyOperator(task_id=SUMMERIZATION_WITH_LLM_FLOW)
    skip_summerization_with_llm_flow = EmptyOperator(
        task_id=SKIP_SUMMERIZATION_WITH_LLM_FLOW
    )

    #####################################################################################################
    #                                          Import Data Task                                         #
    #####################################################################################################

    with TaskGroup(
        "import_data",
    ) as import_data:
        for table_data in TABLES_TO_IMPORT_TO_GCS:
            BigQueryInsertJobOperator(
                project_id=GCP_PROJECT_ID,
                task_id=f"import_{table_data['table_id']}_to_bucket",
                configuration={
                    "extract": {
                        "sourceTable": {
                            "projectId": GCP_PROJECT_ID,
                            "datasetId": table_data["dataset_id"],
                            "tableId": table_data["table_id"],
                        },
                        "compression": None,
                        "destinationUris": os.path.join(
                            STORAGE_BASE_PATH, table_data["filename"]
                        ),
                        "destinationFormat": "PARQUET",
                    }
                },
                dag=dag,
            )

    #####################################################################################################
    #                                      Incremental Update Flow                                      #
    #####################################################################################################

    link_new_products_to_artists = SSHGCEOperator(
        task_id="link_new_products_to_artists",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run cli/link_new_products_to_artists.py \
            --artist-filepath {os.path.join(STORAGE_BASE_PATH, APPLICATIVE_ARTISTS_GCS_FILENAME)} \
            --artist-alias-file-path {os.path.join(STORAGE_BASE_PATH, APPLICATIVE_ARTIST_ALIAS_GCS_FILENAME)} \
            --product-artist-link-filepath {os.path.join(STORAGE_BASE_PATH, APPLICATIVE_PRODUCT_ARTIST_LINK_GCS_FILENAME)} \
            --product-filepath {os.path.join(STORAGE_BASE_PATH, PRODUCTS_TO_LINK_GCS_FILENAME)} \
            --wiki-base-path {WIKIDATA_STORAGE_BASE_PATH} \
            --wiki-file-name {WIKIDATA_EXTRACTION_GCS_FILENAME} \
            --output-delta-artist-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_GCS_FILENAME)} \
            --output-delta-artist-alias-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTIST_ALIAS_GCS_FILENAME)} \
            --output-delta-product-artist-link-filepath {os.path.join(STORAGE_BASE_PATH, DELTA_PRODUCT_ARTIST_LINK_GCS_FILENAME)}
            """,
    )

    #####################################################################################################
    #                                      Refresh Metadatas Flow                                       #
    #####################################################################################################

    refresh_artist_metadatas = SSHGCEOperator(
        task_id="refresh_artist_metadatas",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run cli/refresh_artist_metadatas.py \
            --artist-file-path {os.path.join(STORAGE_BASE_PATH, APPLICATIVE_ARTISTS_GCS_FILENAME)} \
            --artist-alias-file-path {os.path.join(STORAGE_BASE_PATH, APPLICATIVE_ARTIST_ALIAS_GCS_FILENAME)} \
            --wiki-base-path {WIKIDATA_STORAGE_BASE_PATH} \
            --wiki-file-name {WIKIDATA_EXTRACTION_GCS_FILENAME} \
            --output-delta-artist-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_GCS_FILENAME)} \
            --output-delta-artist-alias-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTIST_ALIAS_GCS_FILENAME)} \
            --output-delta-product-artist-link-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_PRODUCT_ARTIST_LINK_GCS_FILENAME)}
            """,
    )

    #####################################################################################################
    #                              Common Incremental + Refresh Metadata                                #
    #####################################################################################################

    get_wikimedia_commons_license_on_delta_tables = SSHGCEOperator(
        task_id="get_wikimedia_commons_license_on_delta_tables",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        trigger_rule="none_failed_min_one_success",
        command=f"""
             uv run cli/get_wikimedia_commons_license.py \
            --artists-matched-on-wikidata {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_WITH_METADATA_GCS_FILENAME)}
            """,
    )

    get_wikipedia_page_content_on_delta_tables = SSHGCEOperator(
        task_id="get_wikipedia_page_content_on_delta_tables",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        trigger_rule="none_failed_min_one_success",
        command=f"""
             uv run cli/get_wikipedia_page_content.py \
            --artists-matched-on-wikidata {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_WITH_METADATA_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_WITH_WIKIPEDIA_PAGE_CONTENT_GCS_FILENAME)}
            """,
    )

    summarize_biographies_with_llm_on_delta_tables = SSHGCEOperator(
        task_id="summarize_biographies_with_llm_on_delta_tables",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run cli/summarize_biographies_with_llm.py \
            --artists-with-wikipedia-content {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_WITH_WIKIPEDIA_PAGE_CONTENT_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_WITH_BIOGRAPHY_GCS_FILENAME)} \
            {SUMMARIZE_BIOGRAPHY_OPTIONS[ENV_SHORT_NAME]}
            """,
    )

    #####################################################################################################
    #                                     Skip LLM Summerization                                        #
    #####################################################################################################

    retrieve_artist_biographies_from_artist_table = SSHGCEOperator(
        task_id="retrieve_artist_biographies_from_artist_table",
        instance_name=GCE_INSTANCE,
        base_dir=BASE_DIR,
        command=f"""
             uv run cli/retrieve_artist_biographies_from_artist_table.py \
            --applicative-artist-file-path {os.path.join(STORAGE_BASE_PATH, APPLICATIVE_ARTISTS_GCS_FILENAME)} \
            --artist-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_GCS_FILENAME)} \
            --output-file-path {os.path.join(STORAGE_BASE_PATH, DELTA_ARTISTS_WITH_BIOGRAPHY_GCS_FILENAME)}
            """,
    )

    #####################################################################################################
    #                                     Load Data into BigQuery                                       #
    #####################################################################################################

    with TaskGroup(
        "load_artist_data_into_delta_tables",
        default_args={"trigger_rule": "none_failed_min_one_success"},
    ) as load_artist_data_into_delta_tables:
        for table_data in GCS_TO_DELTA_TABLES:
            GCSToBigQueryOperator(
                task_id=f"load_data_into_{table_data['table_id']}_table",
                project_id=GCP_PROJECT_ID,
                bucket=ML_BUCKET_TEMP,
                source_objects=os.path.join(GCS_FOLDER_PATH, table_data["filename"]),
                destination_project_dataset_table=f"{BIGQUERY_ML_PREPROCESSING_DATASET}.{table_data['table_id']}",
                source_format="PARQUET",
                write_disposition="WRITE_TRUNCATE",
                autodetect=True,
            )

    #####################################################################################################
    #                                        DAG Dependencies                                           #
    #####################################################################################################

    # Common tasks
    (
        dag_init
        >> vm_init
        >> import_data
        >> choose_linkage
        >> [incremental_flow, refresh_metadata_flow]
    )

    # Incremental Update Flow
    (
        incremental_flow
        >> link_new_products_to_artists
        >> get_wikimedia_commons_license_on_delta_tables
        >> choose_llm_summerization
    )

    # Refresh Metadata Flow
    (
        refresh_metadata_flow
        >> refresh_artist_metadatas
        >> get_wikimedia_commons_license_on_delta_tables
        >> choose_llm_summerization
    )

    # LLM Summerization Choice
    (
        choose_llm_summerization
        >> [
            skip_summerization_with_llm_flow,
            summerization_with_llm_flow,
        ]
    )

    # Skip LLM Summerization Flow
    (
        skip_summerization_with_llm_flow
        >> retrieve_artist_biographies_from_artist_table
        >> load_artist_data_into_delta_tables
    )

    # Summerization with LLM Flow
    (
        summerization_with_llm_flow
        >> get_wikipedia_page_content_on_delta_tables
        >> summarize_biographies_with_llm_on_delta_tables
        >> load_artist_data_into_delta_tables
    )

    # Common end tasks
    (load_artist_data_into_delta_tables >> join_before_stop)

    join_before_stop >> gce_instance_stop
