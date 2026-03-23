from math import ceil

import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
from loguru import logger

from app.retrieval.documents import Document, DocumentArray
from src.constants import (
    ITEM_COLUMNS,
    LANCE_DB_BATCH_SIZE,
    OUTPUT_DATA_PATH,
)
from src.document_processing import get_item_docs, get_user_docs


def _to_ts(f):
    """Convert datetime to timestamp."""
    try:
        return float(f.timestamp())
    except Exception:
        return 0.0


def _to_float(f):
    """Safely convert value to float. Returns None if conversion fails."""
    try:
        return float(f)
    except Exception:
        return None


def _get_table_batches(
    item_embedding_dict: dict, items_df: pd.DataFrame, emb_size: int, total_size: int
) -> pa.RecordBatch:
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
                    pa.array([_to_float(row.is_numerical)], pa.float32()),
                    pa.array([_to_float(row.is_geolocated)], pa.float32()),
                    pa.array([_to_float(row.is_underage_recommendable)], pa.float32()),
                    pa.array([_to_float(row.is_restrained)], pa.float32()),
                    pa.array([_to_float(row.is_sensitive)], pa.float32()),
                    pa.array([_to_float(row.offer_is_duo)], pa.float32()),
                    pa.array([_to_float(row.booking_number)], pa.float32()),
                    pa.array([_to_float(row.booking_number_last_7_days)], pa.float32()),
                    pa.array(
                        [_to_float(row.booking_number_last_14_days)], pa.float32()
                    ),
                    pa.array(
                        [_to_float(row.booking_number_last_28_days)], pa.float32()
                    ),
                    pa.array([_to_float(row.semantic_emb_mean)], pa.float32()),
                    pa.array([_to_float(row.stock_price)], pa.float32()),
                    pa.array([_to_ts(row.offer_creation_date)], pa.float32()),
                    pa.array([_to_ts(row.stock_beginning_date)], pa.float32()),
                    # if unique
                    pa.array([_to_float(row.total_offers)], pa.float32()),
                    pa.array([str(row.example_offer_id)], pa.utf8()),
                    pa.array([str(row.example_offer_name)], pa.utf8()),
                    pa.array([str(row.example_venue_id)], pa.utf8()),
                    pa.array([_to_float(row.example_venue_latitude)], pa.float32()),
                    pa.array([_to_float(row.example_venue_longitude)], pa.float32()),
                ],
                ITEM_COLUMNS,
            )


def _create_items_table(
    item_embedding_dict: dict,
    items_df: pd.DataFrame,
    emb_size: int,
    uri: str,
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
            _get_table_batches(
                item_embedding_dict, batch_df, emb_size, total_size=len(items_df)
            )
        )

        if i == 0:
            table = db.create_table("items", data=data_batch)
        else:
            table.add(data_batch)

    logger.info(
        f"Creating indexes for table 'items' with metric '{vector_search_index_metric}'..."
    )
    table.create_index(
        metric=vector_search_index_metric,
        num_partitions=8,
        num_sub_vectors=4,
        vector_column_name="vector",
    )
    table.create_scalar_index("search_group_name", index_type="BITMAP")
    table.create_scalar_index("subcategory_id", index_type="BITMAP")
    table.create_scalar_index("stock_price", index_type="BTREE")
    logger.info("Indexes created.")


##################################################################################################
#######           Public functions to create lanceDB tables from different sources         #######
##################################################################################################


def create_lancedb_from_coreservation(
    user_embedding_dict: dict[str, np.ndarray],
    item_embedding_dict: dict[str, np.ndarray],
    item_metadatas_df: pd.DataFrame,
    vector_search_metric: str,
):
    """Create lanceDB table from co-reservation model embeddings."""
    # build user and item documents
    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save(f"{OUTPUT_DATA_PATH}/user.docs")
    item_docs = get_item_docs(item_embedding_dict, item_metadatas_df)
    item_docs.save(f"{OUTPUT_DATA_PATH}/item.docs")

    _create_items_table(
        item_embedding_dict,
        item_metadatas_df,
        emb_size=len(next(iter(item_embedding_dict.values()))),
        uri=f"{OUTPUT_DATA_PATH}/vector",
        vector_search_index_metric=vector_search_metric,
    )


def create_lancedb_from_item_embeddings(
    items_with_embeddings_df: pd.DataFrame,
    vector_search_metric: str,
):
    """Create lanceDB table from item embeddings."""
    # Create item documents for graph retrieval app
    item_docs = DocumentArray(
        [
            Document(id=row.item_id, embedding=row.vector)
            for row in items_with_embeddings_df.itertuples()
        ]
    )
    item_docs.save(f"{OUTPUT_DATA_PATH}/item.docs")

    # Create lanceDB table for graph retrieval
    _create_items_table(
        item_embedding_dict={
            row.item_id: row.vector for row in items_with_embeddings_df.itertuples()
        },
        items_df=items_with_embeddings_df.loc[:, lambda df: df.columns != "vector"],
        emb_size=len(items_with_embeddings_df.iloc[0].vector),
        uri=f"{OUTPUT_DATA_PATH}/vector",
        vector_search_index_metric=vector_search_metric,
    )
