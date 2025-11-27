import json
import os
import subprocess
import time
from datetime import datetime
from math import ceil

import lancedb
import pandas as pd
import pyarrow as pa
from docarray import Document, DocumentArray
from google.cloud import bigquery
from loguru import logger

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
BIGQUERY_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"
LANCE_DB_BATCH_SIZE = 100_000
OUTPUT_DATA_PATH = "./metadata"
MODEL_BASE_PATH = "./model"


ITEM_COLUMNS = [
    "vector",
    "item_id",
    "booking_number_desc",
    "booking_trend_desc",
    "booking_creation_trend_desc",
    "booking_release_trend_desc",
    "raw_embeddings",
    "topic_id",
    "cluster_id",
    "category",
    "subcategory_id",
    "search_group_name",
    "offer_type_label",
    "offer_type_domain",
    "gtl_id",
    "gtl_l1",
    "gtl_l2",
    "gtl_l3",
    "gtl_l4",
    "is_numerical",
    "is_geolocated",
    "is_underage_recommendable",
    "is_restrained",
    "is_sensitive",
    "offer_is_duo",
    "booking_number",
    "booking_number_last_7_days",
    "booking_number_last_14_days",
    "booking_number_last_28_days",
    "semantic_emb_mean",
    "stock_price",
    "offer_creation_date",
    "stock_beginning_date",
    "total_offers",
    "example_offer_id",
    "example_offer_name",
    "example_venue_id",
    "example_venue_latitude",
    "example_venue_longitude",
    "artist_id_list",
    "series_id",
]


def download_model(artifact_uri: str) -> None:
    """
    Download model from GCS bucket
    Args:
        artifact_uri (str): GCS bucket path
    """
    command = f"gsutil -m cp -r {artifact_uri} ."
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}: {e.output}")
        raise


def save_experiment(experiment_name, model_name, serving_container, run_id):
    """
    Save experiment results to BigQuery.

    Args:
        experiment_name (str): Name of the MLflow experiment
        model_name (str): Name of the model
        serving_container (str): Container image for serving
        run_id (str): MLflow run ID
    """
    log_results = {
        "execution_date": datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "model_name": model_name,
        "model_type": "custom",
        "run_id": run_id,
        "run_start_time": int(time.time() * 1000.0),
        "run_end_time": int(time.time() * 1000.0),
        "artifact_uri": None,
        "serving_container": serving_container,
    }

    client = bigquery.Client()
    table_id = f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}"""

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("execution_date", "STRING"),
            bigquery.SchemaField("experiment_name", "STRING"),
            bigquery.SchemaField("model_name", "STRING"),
            bigquery.SchemaField("model_type", "STRING"),
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("run_start_time", "INTEGER"),
            bigquery.SchemaField("run_end_time", "INTEGER"),
            bigquery.SchemaField("artifact_uri", "STRING"),
            bigquery.SchemaField("serving_container", "STRING"),
        ]
    )
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    df = pd.DataFrame.from_dict([log_results], orient="columns")

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


def deploy_container(serving_container, workers):
    """
    Deploy container to Docker registry.

    Args:
        serving_container (str): Container image to deploy
        workers (int): Number of workers for the container

    Raises:
        subprocess.CalledProcessError: If deployment fails
    """
    command = f"sh ./deploy_to_docker_registery.sh {serving_container} {workers}"
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}: {e.output}")
        raise


def get_items_metadata():
    """
    Retrieve item metadata from BigQuery.

    Fetches comprehensive item information including categories, bookings,
    embeddings, and venue details from the recommendable_item table.

    Returns:
        pd.DataFrame: DataFrame containing item metadata with columns for
            item_id, categories, booking statistics, embeddings, and venue information
    """
    client = bigquery.Client()

    sql = f"""
        SELECT
        item_id,
        topic_id,
        cluster_id,
        category,
        subcategory_id,
        search_group_name,
        is_numerical,
        is_geolocated,
        offer_is_duo,
        offer_type_domain,
        offer_type_label,
        gtl_id,
        gtl_l1,
        gtl_l2,
        gtl_l3,
        gtl_l4,
        booking_number,
        booking_number_last_7_days,
        booking_number_last_14_days,
        booking_number_last_28_days,
        is_underage_recommendable,
        is_sensitive,
        is_restrained,
        offer_creation_date,
        stock_beginning_date,
        stock_price,
        total_offers,
        semantic_emb_mean,
        example_offer_name,
        example_offer_id,
        example_venue_id,
        example_venue_longitude,
        example_venue_latitude,
        booking_trend,
        stock_date_penalty_factor,
        creation_date_penalty_factor,
        booking_release_trend,
        booking_creation_trend,
        ROW_NUMBER() OVER (ORDER BY booking_number DESC) as booking_number_desc,
        ROW_NUMBER() OVER (ORDER BY booking_trend DESC) as booking_trend_desc,
        ROW_NUMBER() OVER (ORDER BY booking_creation_trend DESC) as booking_creation_trend_desc,
        ROW_NUMBER() OVER (ORDER BY booking_release_trend DESC) as booking_release_trend_desc
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_RECOMMENDATION_DATASET}.recommendable_item`
    """
    return client.query(sql).to_dataframe()


def get_users_dummy_metadata():
    """
    Retrieve user IDs from BigQuery for dummy metadata generation.

    Returns:
        pd.DataFrame: DataFrame containing user_id column
    """
    client = bigquery.Client()

    sql = f"""
        SELECT
            user_id
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.global_user`
    """
    return client.query(sql).to_dataframe()


def to_ts(f):
    """
    Convert datetime to timestamp.

    Args:
        f: Datetime object to convert

    Returns:
        float: Unix timestamp or 0.0 if conversion fails
    """
    try:
        return float(f.timestamp())
    except Exception:
        return 0.0


def to_float(f):
    """
    Safely convert value to float.

    Args:
        f: Value to convert to float

    Returns:
        float or None: Converted float value or None if conversion fails
    """
    try:
        return float(f)
    except Exception:
        return None


def save_model_type(model_type: dict, output_dir: str):
    """
    Save model type configuration to JSON file.

    Args:
        model_type (dict): Dictionary containing model type configuration
        output_dir (str): Directory path where the JSON file will be saved
    """
    with open(f"{output_dir}/model_type.json", "w") as file:
        json.dump(model_type, file)


def get_table_batches(
    item_embedding_dict: dict, items_df: pd.DataFrame, emb_size: int, total_size: int
):
    """
    Generate PyArrow RecordBatch objects for LanceDB table creation.

    Yields batches of item data with embeddings and metadata formatted as
    PyArrow RecordBatches for efficient table insertion.

    Args:
        item_embedding_dict (dict): Mapping of item_id to embedding vectors
        items_df (pd.DataFrame): DataFrame containing item metadata
        emb_size (int): Dimensionality of embeddings
        total_size (int): Total number of items

    Yields:
        pa.RecordBatch: PyArrow RecordBatch containing item data with embeddings
            and metadata fields defined in ITEM_COLUMNS
    """
    preprocessed_items_df = items_df.fillna(
        {
            "topic_id": "",
            "cluster_id": "",
            "category": "",
            "subcategory_id": "",
            "search_group_name": "",
            "offer_type_label": "",
            "offer_type_domain": "",
            "gtl_id": "",
            "gtl_l1": "",
            "gtl_l2": "",
            "gtl_l3": "",
            "gtl_l4": "",
            "example_offer_id": "",
            "example_offer_name": "",
            "example_venue_id": "",
            "example_venue_latitude": 0.0,
            "example_venue_longitude": 0.0,
            "artist_id_list": "",
            "series_id": "",
        }
    )

    for row in preprocessed_items_df.itertuples():
        embedding_id = item_embedding_dict.get(row.item_id, None)
        if embedding_id is not None:
            _item_id = row.item_id
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([embedding_id], pa.list_(pa.float32(), emb_size)),
                    pa.array([_item_id], pa.utf8()),
                    pa.array(
                        [[float(row.booking_number_desc)]],
                        pa.list_(pa.float32(), 1),
                    ),
                    pa.array(
                        [[float(row.booking_trend_desc)]],
                        pa.list_(pa.float32(), 1),
                    ),
                    pa.array(
                        [[float(row.booking_creation_trend_desc)]],
                        pa.list_(pa.float32(), 1),
                    ),
                    pa.array(
                        [[float(row.booking_release_trend_desc)]],
                        pa.list_(pa.float32(), 1),
                    ),
                    pa.array([embedding_id], pa.list_(pa.float32(), emb_size)),
                    pa.array([str(row.topic_id)], pa.utf8()),
                    pa.array([str(row.cluster_id)], pa.utf8()),
                    pa.array([str(row.category)], pa.utf8()),
                    pa.array([str(row.subcategory_id)], pa.utf8()),
                    pa.array([str(row.search_group_name)], pa.utf8()),
                    pa.array([str(row.offer_type_label)], pa.utf8()),
                    pa.array([str(row.offer_type_domain)], pa.utf8()),
                    pa.array([str(row.gtl_id)], pa.utf8()),
                    pa.array([str(row.gtl_l1)], pa.utf8()),
                    pa.array([str(row.gtl_l2)], pa.utf8()),
                    pa.array([str(row.gtl_l3)], pa.utf8()),
                    pa.array([str(row.gtl_l4)], pa.utf8()),
                    pa.array([to_float(row.is_numerical)], pa.float32()),
                    pa.array([to_float(row.is_geolocated)], pa.float32()),
                    pa.array([to_float(row.is_underage_recommendable)], pa.float32()),
                    pa.array([to_float(row.is_restrained)], pa.float32()),
                    pa.array([to_float(row.is_sensitive)], pa.float32()),
                    pa.array([to_float(row.offer_is_duo)], pa.float32()),
                    pa.array([to_float(row.booking_number)], pa.float32()),
                    pa.array([to_float(row.booking_number_last_7_days)], pa.float32()),
                    pa.array([to_float(row.booking_number_last_14_days)], pa.float32()),
                    pa.array([to_float(row.booking_number_last_28_days)], pa.float32()),
                    pa.array([to_float(row.semantic_emb_mean)], pa.float32()),
                    pa.array([to_float(row.stock_price)], pa.float32()),
                    pa.array([to_ts(row.offer_creation_date)], pa.float32()),
                    pa.array([to_ts(row.stock_beginning_date)], pa.float32()),
                    # if unique
                    pa.array([to_float(row.total_offers)], pa.float32()),
                    pa.array([str(row.example_offer_id)], pa.utf8()),
                    pa.array([str(row.example_offer_name)], pa.utf8()),
                    pa.array([str(row.example_venue_id)], pa.utf8()),
                    pa.array([to_float(row.example_venue_latitude)], pa.float32()),
                    pa.array([to_float(row.example_venue_longitude)], pa.float32()),
                    pa.array([str(row.artist_id_list)], pa.utf8()),
                    pa.array([to_float(row.series_id)], pa.float32()),
                ],
                ITEM_COLUMNS,
            )


def create_items_table(
    item_embedding_dict: dict,
    items_df: pd.DataFrame,
    emb_size: int,
    uri: str,
    create_index: bool,
    vector_search_index_metric: str,
) -> None:
    """
    Create a LanceDB table with item embeddings and metadata.

    Processes items in batches to create a LanceDB table with vector embeddings
    and associated metadata. Optionally creates indexes for efficient similarity
    search and filtering.

    Args:
        item_embedding_dict (dict): Mapping of item_id to embedding vectors
        items_df (pd.DataFrame): DataFrame containing item metadata
        emb_size (int): Dimensionality of the embedding vectors
        uri (str): LanceDB database URI/path
        batch_size (int, optional): Number of items per batch. Defaults to LANCE_DB_BATCH_SIZE
        create_index (bool, optional): Whether to create indexes after table creation.
        vector_search_index_metric (str, optional): Metric for vector index.

    Returns:
        None

    TODO: refacto to have only a dataframe containing items and vectors
    TODO: investigate if retrieval work bettes with cosine indexes
    """
    num_batches = ceil(len(items_df) / LANCE_DB_BATCH_SIZE)
    db = lancedb.connect(uri)
    db.drop_database()

    for i in range(num_batches):
        print(
            f"Processing batch {i + 1} // {num_batches} of batch_size {LANCE_DB_BATCH_SIZE}"
        )
        start_idx = i * LANCE_DB_BATCH_SIZE
        end_idx = min((i + 1) * LANCE_DB_BATCH_SIZE, len(items_df))
        batch_df = items_df[start_idx:end_idx]

        data_batch = pa.Table.from_batches(
            get_table_batches(
                item_embedding_dict, batch_df, emb_size, total_size=len(items_df)
            )
        )

        if i == 0:
            table = db.create_table("items", data=data_batch)
        else:
            table.add(data_batch)
    if create_index:
        table.create_index(
            metric=vector_search_index_metric,
            num_partitions=8,
            num_sub_vectors=4,
            vector_column_name="vector",
        )
        table.create_scalar_index("search_group_name", index_type="BITMAP")
        table.create_scalar_index("subcategory_id", index_type="BITMAP")
        table.create_scalar_index("stock_price", index_type="BTREE")


def get_item_docs(item_embedding_dict: dict, items_df: pd.DataFrame) -> DocumentArray:
    """
    Create DocumentArray from item embeddings.

    Builds a DocumentArray containing Documents with item IDs and their
    corresponding embedding vectors.

    Args:
        item_embedding_dict (dict): Mapping of item_id to embedding vectors
        items_df (pd.DataFrame): DataFrame containing item metadata

    Returns:
        DocumentArray: Collection of Documents with item embeddings

    Raises:
        Exception: If no valid documents are created (empty DocumentArray)
    """
    docs = DocumentArray()
    for row in items_df.itertuples():
        item_id = row.item_id
        embedding_id = item_embedding_dict.get(row.item_id)
        if embedding_id is not None:
            docs.append(Document(id=str(item_id), embedding=embedding_id))

    if len(docs) == 0:
        raise Exception("Item Document is empty. Does the model match the query ?")

    return docs


def get_user_docs(user_dict: dict) -> DocumentArray:
    """
    Create DocumentArray from user embeddings.

    Args:
        user_dict (dict): Mapping of user_id to embedding vectors

    Returns:
        DocumentArray: Collection of Documents with user embeddings
    """
    docs = DocumentArray()
    for k, v in user_dict.items():
        docs.append(Document(id=str(k), embedding=v))
    return docs


def get_model_from_mlflow(
    experiment_name: str, run_id: str = None, artifact_uri: str = None
):
    """
    Retrieve model artifact URI from MLflow via BigQuery.

    Fetches the artifact URI for a model from BigQuery's MLflow results table,
    either by experiment name (latest run) or by specific run_id.

    Args:
        experiment_name (str): Name of the MLflow experiment
        run_id (str, optional): Specific MLflow run ID. Defaults to None
        artifact_uri (str, optional): Direct artifact URI (returned if valid).
            Defaults to None

    Returns:
        str: GCS artifact URI for the model

    Raises:
        Exception: If model is not found in BigQuery results table
    """
    client = bigquery.Client()

    # get artifact_uri from BQ
    if artifact_uri is None or len(artifact_uri) <= 10:
        if run_id is None or len(run_id) <= 2:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        else:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        if len(results_array) == 0:
            raise Exception(
                f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}. Failing."
            )
        else:
            artifact_uri = results_array[0]["artifact_uri"]
    return artifact_uri
