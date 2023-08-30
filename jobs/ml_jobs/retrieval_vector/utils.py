import os
import pandas as pd
from datetime import datetime
import time
import subprocess
from docarray import DocumentArray, Document
import json

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


def deploy_container(serving_container, workers):
    command = f"sh ./deploy_to_docker_registery.sh {serving_container} {workers}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def get_items_metadata():
    sql = f"""
        SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.recommendable_items_raw`
    """
    return pd.read_gbq(sql)


def get_users_metadata():
    sql = f"""
        SELECT 
            user_id,
            user_total_deposit_amount,
            user_current_deposit_type,
            COALESCE(user_theoretical_remaining_credit, user_last_deposit_amount) as user_theoretical_remaining_credit
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_ANALYTICS_DATASET}.enriched_user_data` 
    """
    return pd.read_gbq(sql)


def to_ts(f):
    try:
        return float(f.timestamp())
    except:
        return 0.0


def save_model_type(model_type):
    with open("./metadata/model_type.json", "w") as file:
        json.dump(model_type, file)


def get_item_docs(item_embedding_dict, items_df):
    docs = DocumentArray()
    for row in items_df.itertuples():
        embedding_id = item_embedding_dict.get(row.item_id, None)
        if embedding_id is not None:
            _item_id = row.item_id
            metadata = {
                "item_id": str(_item_id),
                "category": str(row.category or ""),
                "subcategory_id": str(row.subcategory_id or ""),
                "search_group_name": str(row.search_group_name or ""),
                "offer_type_label": str(row.offer_type_label or ""),
                # "offer_type_labels": str(row.offer_type_labels or "").split(";"),
                "offer_type_domain": str(row.offer_type_domain or ""),
                "is_numerical": float(row.is_numerical),
                "is_national": float(row.is_national),
                "is_geolocated": float(row.is_geolocated),
                "is_underage_recommendable": float(row.is_underage_recommendable),
                "offer_is_duo": float(row.offer_is_duo),
                "booking_number": float(row.booking_number),
                "booking_number_last_7_days": float(row.booking_number_last_7_days),
                "booking_number_last_14_days": float(row.booking_number_last_14_days),
                "booking_number_last_28_days": float(row.booking_number_last_28_days),
                "stock_price": float(row.stock_price),
                "offer_creation_date": to_ts(row.offer_creation_date),
                "stock_beginning_date": to_ts(row.stock_beginning_date),
                "example_offer_id": str(row.example_offer_id),
                "example_offer_name": str(row.example_offer_name),
            }
            docs.append(
                Document(id=str(_item_id), tags=metadata, embedding=embedding_id)
            )

    if len(docs) == 0:
        raise Exception("Item Document is empty. Does the model match the query ?")

    return docs


def get_user_docs(user_dict):
    docs = DocumentArray()
    for k, v in user_dict.items():
        docs.append(Document(id=str(k), embedding=v))
    return docs
