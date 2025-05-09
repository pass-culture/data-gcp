import time
from typing import Dict, List

from google.cloud import aiplatform, bigquery
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Value

# Constants
PROJECT = "passculture-data-prod"
LOCATION = "europe-west1"
API_ENDPOINT = "europe-west1-aiplatform.googleapis.com"
ENDPOINT_IDS = {
    # "prod": "4777694682035519488",
    "qi_scalar": "4160424456155561984",  # replace
    # "qi_1": "1706068212354908160",  # replace
}
BQ_DATASET = "sandbox_prod"
BQ_TABLE_PREFIX = "retrieval_results"  # e.g., retrieval_results_model_1

# Initialize clients
prediction_client = aiplatform.gapic.PredictionServiceClient(
    client_options={"api_endpoint": API_ENDPOINT}
)
bq_client = bigquery.Client(project=PROJECT)

# Read 200k user_ids
with open("DPP_eval_200k_user_list.csv") as f:
    user_ids = [line.strip() for line in f if line.strip()]


# Utility to call Vertex AI
def get_predictions(user_id: str, endpoint_id: str):
    instance = {
        "model_type": "recommendation",
        "user_id": user_id,
        "size": 60,
        "params": {},
        "call_id": "call_" + user_id,
        "debug": 1,
        "prefilter": 1,
        "similarity_metric": "dot",
    }
    instances = [json_format.ParseDict(instance, Value())]
    endpoint_path = prediction_client.endpoint_path(
        project=PROJECT, location=LOCATION, endpoint=endpoint_id
    )

    try:
        response = prediction_client.predict(
            endpoint=endpoint_path, instances=instances
        )
        predictions = []
        for pred in response.predictions:
            predictions.append(
                {
                    "user_id": user_id,
                    "item_id": pred["item_id"],
                    "score": pred["_distance"],
                }
            )
        return predictions
    except Exception as e:
        print(f"Error with user {user_id} on endpoint {endpoint_id}: {e}")
        return []


def ensure_bq_table_exists(project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception:
        print(f"Creating table {table_id}...")
        schema = [
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("item_id", "STRING"),
            bigquery.SchemaField("score", "FLOAT"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created.")


# Upload to BigQuery
def upload_to_bigquery(rows: List[Dict], table_name: str):
    table_id = f"passculture-data-prod.sandbox_prod.{table_name}"
    print(table_id)
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"Encountered errors while inserting to {table_id}: {errors}")
    else:
        print(f"Uploaded {len(rows)} rows to {table_id}")


for model_name, endpoint_id in ENDPOINT_IDS.items():
    ensure_bq_table_exists(
        "passculture-data-prod", "sandbox_prod", f"{BQ_TABLE_PREFIX}_{model_name}"
    )

time.sleep(60)  # sleep till tables are ready

# Main loop
BATCH_SIZE = 500  # Adjust as needed to balance rate limits and performance
for model_name, endpoint_id in ENDPOINT_IDS.items():
    print(f"Processing model: {model_name}")
    all_results = []

    for i in range(0, len(user_ids), BATCH_SIZE):
        batch_ids = user_ids[i : i + BATCH_SIZE]
        batch_results = []

        for uid in batch_ids:
            preds = get_predictions(uid, endpoint_id)
            batch_results.extend(preds)

        all_results.extend(batch_results)
        print(
            f"Processed batch {i // BATCH_SIZE + 1}, got {len(batch_results)} predictions"
        )
        time.sleep(1)  # Respect API rate limits

        # Optionally: upload in chunks
        if len(all_results) >= 5000:
            upload_to_bigquery(all_results, f"{BQ_TABLE_PREFIX}_{model_name}")
            all_results = []

    # Upload any remaining results
    if all_results:
        upload_to_bigquery(all_results, f"{BQ_TABLE_PREFIX}_{model_name}")
