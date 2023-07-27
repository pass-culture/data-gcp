import os
import pandas as pd
from datetime import datetime
import time
import subprocess
from docarray import DocumentArray, Document
import uuid
import json
import numpy as np

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"


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
    pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
        project_id=f"{GCP_PROJECT_ID}",
        if_exists="append",
    )


def deploy_container(serving_container):
    command = f"sh ./deploy_to_docker_registery.sh {serving_container}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def get_items_metadata():
    sql = f"""
        WITH offer_details AS (
            SELECT 
                item_id,
                offer_id, 
                offer_name,
                booking_confirm_cnt
            FROM `passculture-data-prod.analytics_prod.enriched_offer_data` 
            QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY booking_confirm_cnt DESC) = 1
            ) 

        SELECT ri.*, od.name, od.booking_confirm_cnt, od.offer_id
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.recommendable_items_raw` ro
        LEFT JOIN offer_details od on od.item_id = ri.item_id
    """
    return pd.read_gbq(sql)


def to_ts(f):
    try:
        return int(f.timestamp())
    except:
        return None


def save_model_type(model_type):
    with open("./metadata/model_type.json", "w") as file:
        json.dump(model_type, file)


def get_item_docs(item_embedding_dict, items_df, n_dim, metric):
    docs = DocumentArray(
        storage="annlite",
        config={
            "n_dim": n_dim,
            "metric": metric,
            "data_path": f"./metadata/annlite_{str(uuid.uuid4())}",
            "ef_construction": 200,  # default
            "ef_search": 500,  # limit size
            "max_connection": 48,  # higher better
            "columns": {
                "category": "str",
                "subcategory_id": "str",
                "search_group_name": "str",
                "is_numerical": "int",
                "is_national": "int",
                "is_geolocated": "int",
                "offer_is_duo": "int",
                "booking_number": "float",
                "stock_price": "float",
                "offer_creation_date": "int",
                "stock_beginning_date": "int",
            },
        },
    )

    for row in items_df.itertuples():
        embedding_id = item_embedding_dict.get(row.item_id, None)
        if embedding_id is not None:
            _item_id = row.item_id
            metadata = {
                "item_id": _item_id,
                "category": row.category,
                "subcategory_id": row.subcategory_id,
                "search_group_name": row.search_group_name,
                "is_numerical": int(row.is_numerical),
                "is_national": int(row.is_national),
                "is_geolocated": int(row.is_geolocated),
                "offer_is_duo": int(row.offer_is_duo),
                "booking_number": float(row.booking_number),
                "stock_price": float(row.stock_price),
                "offer_creation_date": to_ts(row.offer_creation_date),
                "stock_beginning_date": to_ts(row.stock_beginning_date),
                "total_associated_offers": row.total_associated_offers,
                "name": row.name,
            }
            docs.append(Document(id=_item_id, tags=metadata, embedding=embedding_id))
    return docs


def get_user_docs(user_dict):
    docs = DocumentArray()

    for k, v in user_dict.items():
        # TODO : dot product / negative
        docs.append(Document(id=k, embedding=np.negative(v)))
    return docs
