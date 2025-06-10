import os
from datetime import datetime, timedelta

from common import macros
from common.config import (
    BIGQUERY_ML_LINKAGE_DATASET,
    BIGQUERY_SANDBOX_DATASET,
    BIGQUERY_TMP_DATASET,
    DAG_FOLDER,
    DAG_TAGS,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MLFLOW_BUCKET_NAME,
    ML_BUCKET_OUTPUT,
    ML_BUCKET_PROCESSING,
    ML_BUCKET_TEMP,
)
from common.operators.gce import (
    DeleteGCEOperator,
    InstallDependenciesOperator,
    SSHGCEOperator,
    StartGCEOperator,
)
from common.utils import get_airflow_schedule
from jobs.crons import SCHEDULE_DICT
from jobs.ml.constants import IMPORT_LINKAGE_SQL_PATH

from airflow import DAG
from airflow.models import Param
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.task_group import TaskGroup

DATE = "{{ ts_nodash }}"

# -------------------------------------------------------------------------
# DAG CONFIG
# -------------------------------------------------------------------------

DAG_CONFIG = {
    "ID": "link_items",
    "ENVIROMENT": {
        "GCP_PROJECT": GCP_PROJECT_ID,
        "ENV_SHORT_NAME": ENV_SHORT_NAME,
    },
    "BASE_PATHS": {
        "GCS_FOLDER": f"linkage_item_{ENV_SHORT_NAME}/linkage_{DATE}",
        "STORAGE": f"gs://{ML_BUCKET_TEMP}/linkage_item_{ENV_SHORT_NAME}/linkage_{DATE}",
    },
    "DIRS": {
        "BASE": "data-gcp/jobs/ml_jobs/item_linkage/",
        "INPUT_SOURCES": "sources_data",
        "INPUT_CANDIDATES": "candidates_data",
        "SOURCES_CLEAN": "sources_clean",
        "CANDIDATES_CLEAN": "candidates_clean",
        "PRODUCT_SOURCES_READY": "product_sources_ready",
        "PRODUCT_CANDIDATES_READY": "product_candidates_ready",
        "OFFER_SOURCES_READY": "offer_sources_ready",
        "OFFER_CANDIDATES_READY": "offer_candidates_ready",
    },
    "FILES": lambda linkage_type: {
        "LINKAGE_CANDIDATES": f"linkage_candidates_{linkage_type}",
        "LINKED": f"linked_{linkage_type}",
        "UNMATCHED": f"unmatched_{linkage_type}",
        "LINKED_W_ID": f"linked_{linkage_type}_w_id",  # for offers
    },
    "BIGQUERY": {
        "INPUT_SOURCES_TABLE": f"{DATE}_input_sources_table",
        "INPUT_CANDIDATES_TABLE": f"{DATE}_input_candidates_data",
        "LINKED_PRODUCT_TABLE": "linked_product",
        "LINKED_OFFER_TABLE": "linked_offer",
        "ITEM_MAPPING_TABLE": "item_offer_mapping",
    },
    "PARAMS": {
        "REDUCTION": "true",
        "BATCH_SIZE": 100000,
    },
}

GCE_PARAMS = {
    "instance_name": f"linkage-item-{ENV_SHORT_NAME}",
    "instance_type": {
        "dev": "n1-standard-2",
        "stg": "n1-standard-8",
        "prod": "n1-standard-32",
    },
}

DEFAULT_ARGS = {
    "start_date": datetime(2022, 11, 30),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------------
def build_path(base_path, sub_path):
    """Helper to build GCS/OS paths."""
    return os.path.join(base_path, sub_path)


def create_ssh_task(task_id, command, instance_name, base_dir):
    """Helper to create an SSHGCEOperator with minimal boilerplate."""
    return SSHGCEOperator(
        task_id=task_id,
        instance_name=instance_name,
        base_dir=base_dir,
        environment=DAG_CONFIG["ENVIROMENT"],
        command=command,
    )


# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------
with DAG(
    DAG_CONFIG["ID"],
    default_args=DEFAULT_ARGS,
    description="Process to link items using semantic vectors (grouped by product/offer).",
    schedule_interval=get_airflow_schedule(
        SCHEDULE_DICT[DAG_CONFIG["ID"]][ENV_SHORT_NAME]
    ),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1440),
    user_defined_macros=macros.default,
    template_searchpath=DAG_FOLDER,
    tags=[DAG_TAGS.DS.value, DAG_TAGS.VM.value],
    params={
        "branch": Param(
            default="production" if ENV_SHORT_NAME == "prod" else "master",
            type="string",
        ),
        "instance_type": Param(
            default=GCE_PARAMS["instance_type"][ENV_SHORT_NAME],
            type="string",
        ),
        "instance_name": Param(
            default=GCE_PARAMS["instance_name"],
            type="string",
        ),
        "reduction": Param(
            default=DAG_CONFIG["PARAMS"]["REDUCTION"],
            type="string",
        ),
        "batch_size": Param(
            default=DAG_CONFIG["PARAMS"]["BATCH_SIZE"],
            type="integer",
        ),
    },
) as dag:
    # ---------------------------------------------------------------------
    # START / END
    # ---------------------------------------------------------------------
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # ---------------------------------------------------------------------
    # 1) IMPORT DATA
    # ---------------------------------------------------------------------
    with TaskGroup(
        "import_data", tooltip="Import data from SQL to BQ"
    ) as import_data_group:
        import_sources = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_sources",
            configuration={
                "query": {
                    "query": (
                        IMPORT_LINKAGE_SQL_PATH / "linkage_item_sources_data.sql"
                    ).as_posix(),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_SOURCES_TABLE"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

        import_candidates = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="import_candidates",
            configuration={
                "query": {
                    "query": (
                        IMPORT_LINKAGE_SQL_PATH / "linkage_item_candidates_data.sql"
                    ).as_posix(),
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_CANDIDATES_TABLE"],
                    },
                    "writeDisposition": "WRITE_TRUNCATE",
                }
            },
        )

    # ---------------------------------------------------------------------
    # 2) EXPORT DATA (FROM BQ TO GCS)
    # ---------------------------------------------------------------------
    with TaskGroup(
        "export_data", tooltip="Export data from BQ to GCS"
    ) as export_data_group:
        export_sources_bq = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="export_sources_bq",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_SOURCES_TABLE"],
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        DAG_CONFIG["BASE_PATHS"]["STORAGE"],
                        DAG_CONFIG["DIRS"]["INPUT_SOURCES"],
                        "data-*.parquet",
                    ),
                }
            },
        )

        export_candidates_bq = BigQueryInsertJobOperator(
            project_id=GCP_PROJECT_ID,
            task_id="export_candidates_bq",
            configuration={
                "extract": {
                    "sourceTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": BIGQUERY_TMP_DATASET,
                        "tableId": DAG_CONFIG["BIGQUERY"]["INPUT_CANDIDATES_TABLE"],
                    },
                    "compression": None,
                    "destinationFormat": "PARQUET",
                    "destinationUris": os.path.join(
                        DAG_CONFIG["BASE_PATHS"]["STORAGE"],
                        DAG_CONFIG["DIRS"]["INPUT_CANDIDATES"],
                        "data-*.parquet",
                    ),
                }
            },
        )

    # ---------------------------------------------------------------------
    # 3) GCE INSTANCE START + CODE FETCH + INSTALL DEPS
    # ---------------------------------------------------------------------
    gce_instance_start = StartGCEOperator(
        task_id="gce_start_task",
        preemptible=False,
        instance_name="{{ params.instance_name }}",
        instance_type="{{ params.instance_type }}",
        retries=2,
        labels={"job_type": "long_ml", "dag_name": DAG_CONFIG["ID"]},
    )

    fetch_install_code = InstallDependenciesOperator(
        task_id="fetch_install_code",
        instance_name="{{ params.instance_name }}",
        branch="{{ params.branch }}",
        python_version="3.10",
        base_dir=DAG_CONFIG["DIRS"]["BASE"],
        retries=2,
    )

    # ---------------------------------------------------------------------
    # 4) PREPROCESS DATA (sources / candidates)
    # ---------------------------------------------------------------------
    with TaskGroup(
        "process_data", tooltip="Preprocess data tasks"
    ) as process_data_group:
        process_sources = create_ssh_task(
            task_id="process_sources",
            command="python preprocess.py "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['INPUT_SOURCES'])}/data-*.parquet "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['SOURCES_CLEAN'])} "
            f"--reduction {DAG_CONFIG['PARAMS']['REDUCTION']} "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        process_candidates = create_ssh_task(
            task_id="process_candidates",
            command="python preprocess.py "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['INPUT_CANDIDATES'])}/data-*.parquet "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['CANDIDATES_CLEAN'])} "
            f"--reduction {DAG_CONFIG['PARAMS']['REDUCTION']} "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

    # ---------------------------------------------------------------------
    # 5) PRODUCT WORKFLOW
    # ---------------------------------------------------------------------
    with TaskGroup(
        "product_workflow", tooltip="All tasks related to product linkage"
    ) as product_workflow:
        prepare_offer_to_product_candidates = create_ssh_task(
            task_id="prepare_offer_to_product_candidates",
            command="python prepare_tables.py "
            "--linkage-type product "
            f"--input-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['CANDIDATES_CLEAN'])}/data-*.parquet "
            f"--output-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_CANDIDATES_READY'])} "
            f"--input-sources-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['SOURCES_CLEAN'])}/data-*.parquet "
            f"--output-sources-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_SOURCES_READY'])} "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 5.1) Build product semantic space
        build_product_linkage_vector = create_ssh_task(
            task_id="build_product_linkage_vector",
            command="python build_semantic_space.py "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_SOURCES_READY'])}/data-*.parquet "
            f"--linkage-type product "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 5.2) Get product linkage candidates
        get_product_linkage_candidates = create_ssh_task(
            task_id="get_product_linkage_candidates",
            command="python linkage_candidates.py "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} "
            f"--linkage-type product "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_CANDIDATES_READY'])}/data-*.parquet "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('product')['LINKAGE_CANDIDATES'])} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 5.3) Link products
        link_products = create_ssh_task(
            task_id="link_products",
            command="python link_items.py "
            "--linkage-type product "
            f"--input-sources-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_SOURCES_READY'])} "
            f"--input-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_CANDIDATES_READY'])} "
            f"--linkage-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('product')['LINKAGE_CANDIDATES'])} "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('product')['LINKED'])} "
            f"--unmatched-elements-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('product')['UNMATCHED'])} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 5.4) Load linked product into BigQuery
        load_linked_product_into_bq = GCSToBigQueryOperator(
            project_id=GCP_PROJECT_ID,
            task_id="load_linked_product_into_bq",
            bucket=MLFLOW_BUCKET_NAME,
            source_objects=f"""{
                build_path(
                    DAG_CONFIG["BASE_PATHS"]["GCS_FOLDER"],
                    DAG_CONFIG["FILES"]("product")["LINKED"],
                )
            }/data.parquet""",
            destination_project_dataset_table=(
                f"{BIGQUERY_SANDBOX_DATASET}.{DAG_CONFIG['BIGQUERY']['LINKED_PRODUCT_TABLE']}"
            ),
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        # 5.5) Evaluate product linkage (optional, if you want it in the same group)
        evaluate_product = create_ssh_task(
            task_id="evaluate_product_linkage",
            command="python evaluate.py "
            f"--input-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['PRODUCT_CANDIDATES_READY'])} "
            f"--linkage-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('product')['LINKED'])} "
            "--linkage-type product ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # Set product-workflow sequence
        (
            prepare_offer_to_product_candidates
            >> build_product_linkage_vector
            >> get_product_linkage_candidates
            >> link_products
            >> load_linked_product_into_bq
            >> evaluate_product
        )

    # ---------------------------------------------------------------------
    # 6) OFFER WORKFLOW
    # ---------------------------------------------------------------------
    with TaskGroup(
        "offer_workflow", tooltip="All tasks related to offer linkage"
    ) as offer_workflow:
        prepare_offer_to_offer_candidates = create_ssh_task(
            task_id="prepare_offer_to_offer_candidates",
            command="python prepare_tables.py "
            "--linkage-type offer "
            f"--input-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['CANDIDATES_CLEAN'])}/data-*.parquet "
            f"--output-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['OFFER_CANDIDATES_READY'])} "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} "
            f"--unmatched-elements-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('product')['UNMATCHED'])} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )
        # 6.1) Build offer semantic space
        build_offer_linkage_vector = create_ssh_task(
            task_id="build_offer_linkage_vector",
            command="python build_semantic_space.py "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['OFFER_CANDIDATES_READY'])}/data-*.parquet "
            f"--linkage-type offer "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 6.2) Get offer linkage candidates
        get_offer_linkage_candidates = create_ssh_task(
            task_id="get_offer_linkage_candidates",
            command="python linkage_candidates.py "
            f"--batch-size {DAG_CONFIG['PARAMS']['BATCH_SIZE']} "
            f"--linkage-type offer "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['OFFER_CANDIDATES_READY'])}/data-*.parquet "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['LINKAGE_CANDIDATES'])} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 6.3) Link offers
        link_offers = create_ssh_task(
            task_id="link_offers",
            command="python link_items.py "
            "--linkage-type offer "
            f"--input-sources-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['OFFER_CANDIDATES_READY'])} "
            f"--input-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['OFFER_CANDIDATES_READY'])} "
            f"--linkage-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['LINKAGE_CANDIDATES'])} "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['LINKED'])} "
            f"--unmatched-elements-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['UNMATCHED'])} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 6.4) Assign IDs to newly linked offers
        assign_linked_ids = create_ssh_task(
            task_id="assign_linked_ids",
            command="python assign_linked_ids.py "
            f"--input-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['LINKED'])} "
            f"--output-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['LINKED_W_ID'])} ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # 6.5) Load linked offers into BigQuery
        load_linked_offer_into_bq = GCSToBigQueryOperator(
            project_id=GCP_PROJECT_ID,
            task_id="load_linked_offer_into_bq",
            bucket=MLFLOW_BUCKET_NAME,
            source_objects=f"""{
                build_path(
                    DAG_CONFIG["BASE_PATHS"]["GCS_FOLDER"],
                    DAG_CONFIG["FILES"]("offer")["LINKED_W_ID"],
                )
            }/data.parquet""",
            destination_project_dataset_table=(
                f"{BIGQUERY_SANDBOX_DATASET}.{DAG_CONFIG['BIGQUERY']['LINKED_OFFER_TABLE']}"
            ),
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        # 6.6) Evaluate offer linkage (optional, if you want it in the same group)
        evaluate_offer = create_ssh_task(
            task_id="evaluate_offer_linkage",
            command="python evaluate.py "
            f"--input-candidates-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['DIRS']['OFFER_CANDIDATES_READY'])} "
            f"--linkage-path {build_path(DAG_CONFIG['BASE_PATHS']['STORAGE'], DAG_CONFIG['FILES']('offer')['LINKED_W_ID'])} "
            "--linkage-type offer ",
            instance_name="{{ params.instance_name }}",
            base_dir=DAG_CONFIG["DIRS"]["BASE"],
        )

        # Set offer-workflow sequence
        (
            prepare_offer_to_offer_candidates
            >> build_offer_linkage_vector
            >> get_offer_linkage_candidates
            >> link_offers
            >> assign_linked_ids
            >> load_linked_offer_into_bq
            >> evaluate_offer
        )
    # - ---------------------------------------------------------------------
    #  7) Export item mapping to BigQuery
    # ---------------------------------------------------------------------
    export_item_mapping = BigQueryInsertJobOperator(
        task_id="export_item_mapping",
        configuration={
            "query": {
                "query": (IMPORT_LINKAGE_SQL_PATH / "build_mapping.sql").as_posix(),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_ML_LINKAGE_DATASET,
                    "tableId": DAG_CONFIG["BIGQUERY"]["ITEM_MAPPING_TABLE"],
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    gce_instance_stop = DeleteGCEOperator(
        task_id="gce_stop_task", instance_name="{{ params.instance_name }}"
    )

    # ---------------------------------------------------------------------
    # DAG SEQUENCE
    # ---------------------------------------------------------------------
    start >> import_data_group >> export_data_group >> gce_instance_start
    (
        gce_instance_start
        >> fetch_install_code
        # >> install_dependencies
        >> process_data_group
    )

    # product_workflow starts after we process data
    process_data_group >> product_workflow

    # offer_workflow also depends on the product workflow’s “link_products”
    # to have the unmatched product ready. We can chain them here:
    product_workflow >> offer_workflow

    # then stop GCE and end
    offer_workflow >> export_item_mapping >> gce_instance_stop >> end
