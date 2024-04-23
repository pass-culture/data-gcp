import os
import pandas as pd
from datetime import datetime
import time
import subprocess
from docarray import DocumentArray, Document
import json
import pyarrow as pa
import lancedb

from google.cloud import bigquery


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"
BIGQUERY_RECOMMENDATION_DATASET = f"ml_reco_{ENV_SHORT_NAME}"

item_columns = [
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
    "is_national",
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
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def get_items_metadata():
    client = bigquery.Client()

    sql = f"""
        SELECT 
        *, 
        ROW_NUMBER() OVER (ORDER BY booking_number DESC) as booking_number_desc,
        ROW_NUMBER() OVER (ORDER BY booking_trend DESC) as booking_trend_desc,
        ROW_NUMBER() OVER (ORDER BY booking_creation_trend DESC) as booking_creation_trend_desc,
        ROW_NUMBER() OVER (ORDER BY booking_release_trend DESC) as booking_release_trend_desc
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_RECOMMENDATION_DATASET}.recommendable_item`
    """
    return client.query(sql).to_dataframe()


def get_users_metadata():
    client = bigquery.Client()

    sql = f"""
        SELECT 
            user_id,
            user_total_deposit_amount,
            user_current_deposit_type,
            COALESCE(user_theoretical_remaining_credit, user_last_deposit_amount) as user_theoretical_remaining_credit
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data` 
    """
    return client.query(sql).to_dataframe()


def to_ts(f):
    try:
        return float(f.timestamp())
    except:
        return 0.0


def to_float(f):
    try:
        return float(f)
    except:
        return None


def save_model_type(model_type):
    with open("./metadata/model_type.json", "w") as file:
        json.dump(model_type, file)


def get_table_batches(item_embedding_dict: dict, items_df, emb_size):
    for row in items_df.itertuples():
        embedding_id = item_embedding_dict.get(row.item_id, None)
        if embedding_id is not None:
            _item_id = row.item_id
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([embedding_id], pa.list_(pa.float32(), emb_size)),
                    pa.array([_item_id], pa.utf8()),
                    pa.array(
                        [[float(row.booking_number_desc)]], pa.list_(pa.float32(), 1)
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
                    pa.array([str(row.topic_id or "")], pa.utf8()),
                    pa.array([str(row.cluster_id or "")], pa.utf8()),
                    pa.array([str(row.category or "")], pa.utf8()),
                    pa.array([str(row.subcategory_id or "")], pa.utf8()),
                    pa.array([str(row.search_group_name or "")], pa.utf8()),
                    pa.array([str(row.offer_type_label or "")], pa.utf8()),
                    pa.array([str(row.offer_type_domain or "")], pa.utf8()),
                    pa.array([str(row.gtl_id or "")], pa.utf8()),
                    pa.array([str(row.gtl_l1 or "")], pa.utf8()),
                    pa.array([str(row.gtl_l2 or "")], pa.utf8()),
                    pa.array([str(row.gtl_l3 or "")], pa.utf8()),
                    pa.array([str(row.gtl_l4 or "")], pa.utf8()),
                    pa.array([to_float(row.is_numerical)], pa.float32()),
                    pa.array([to_float(row.is_national)], pa.float32()),
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
                    pa.array([str(row.example_offer_id or "")], pa.utf8()),
                    pa.array([str(row.example_offer_name or "")], pa.utf8()),
                    pa.array([str(row.example_venue_id or "")], pa.utf8()),
                    pa.array(
                        [to_float(row.example_venue_latitude or 0.0)], pa.float32()
                    ),
                    pa.array(
                        [to_float(row.example_venue_longitude or 0.0)], pa.float32()
                    ),
                ],
                item_columns,
            )


def create_items_table(
    item_embedding_dict, items_df, emb_size, uri="./metadata/vector"
):
    data = pa.Table.from_batches(
        get_table_batches(item_embedding_dict, items_df, emb_size)
    )
    db = lancedb.connect(uri)
    db.drop_database()
    table = db.create_table("items", data=data)
    table.create_index(num_partitions=1024, num_sub_vectors=32)


def get_item_docs(item_embedding_dict, items_df):
    docs = DocumentArray()
    for row in items_df.itertuples():
        embedding_id = item_embedding_dict.get(row.item_id, None)
        if embedding_id is not None:
            _item_id = row.item_id
            docs.append(Document(id=str(_item_id), embedding=embedding_id))

    if len(docs) == 0:
        raise Exception("Item Document is empty. Does the model match the query ?")

    return docs


def get_user_docs(user_dict):
    docs = DocumentArray()
    for k, v in user_dict.items():
        docs.append(Document(id=str(k), embedding=v))
    return docs
