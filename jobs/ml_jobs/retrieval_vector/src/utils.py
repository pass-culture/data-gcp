import json
import subprocess
from math import ceil

import lancedb
import pandas as pd
import pyarrow as pa
from loguru import logger

from app.retrieval.documents import Document, DocumentArray
from src.constants import (
    ITEM_COLUMNS,
    LANCE_DB_BATCH_SIZE,
)


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
