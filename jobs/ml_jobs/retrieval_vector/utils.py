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
    client = bigquery.Client()

    sql = f"""
        SELECT
            user_id
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.global_user`
    """
    return client.query(sql).to_dataframe()


def to_ts(f):
    try:
        return float(f.timestamp())
    except Exception:
        return 0.0


def to_float(f):
    try:
        return float(f)
    except Exception:
        return None


def save_model_type(model_type: dict, output_dir: str):
    with open(f"{output_dir}/model_type.json", "w") as file:
        json.dump(model_type, file)


def get_table_batches(
    item_embedding_dict: dict, items_df: pd.DataFrame, emb_size: int, total_size: int
):
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
    batch_size: int = LANCE_DB_BATCH_SIZE,
    create_index: bool = True,
) -> None:
    num_batches = ceil(len(items_df) / batch_size)
    db = lancedb.connect(uri)
    db.drop_database()

    for i in range(num_batches):
        print(f"Processing batch {i + 1} // {num_batches} of batch_size {batch_size}")
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(items_df))
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
            metric="dot",
            num_partitions=8,
            num_sub_vectors=4,
            vector_column_name="vector",
        )
        table.create_scalar_index("search_group_name", index_type="BITMAP")
        table.create_scalar_index("subcategory_id", index_type="BITMAP")
        table.create_scalar_index("stock_price", index_type="BTREE")


def get_item_docs(item_embedding_dict: dict, items_df: pd.DataFrame) -> DocumentArray:
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
    docs = DocumentArray()
    for k, v in user_dict.items():
        docs.append(Document(id=str(k), embedding=v))
    return docs


def get_model_from_mlflow(
    experiment_name: str, run_id: str = None, artifact_uri: str = None
):
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
