import os
import time
from collections import defaultdict
import numpy as np
import pandas as pd
import tensorflow as tf

from call_endpoint_utils import call_endpoint
from constants import ENV_SHORT_NAME
from utils.tools import (
    download_model,
    get_model_from_mlflow,
    get_data_from_sql_query,
)
from utils.analysis_utils import analyze_predictions

MODEL_BASE_PATH = "model"


# Make multiple calls per user to analyze consistency
def process_users_endpoint_calls(user_id_subset, n_calls_per_user):
    predictions_by_user = defaultdict(list)
    latencies = []
    for user_id in user_id_subset:
        # print(f"\nProcessing user_id: {user_id}")
        for call_num in range(n_calls_per_user):
            start_time = time.time()
            predictions = call_endpoint(
                model_type="recommendation",
                user_id=user_id,
                size=10,
            )
            end_time = time.time()
            latencies.append(end_time - start_time)
            predictions_by_user[user_id].append(predictions)
            print(f"Call {call_num + 1} completed")
    return predictions_by_user, latencies


## MAIN EXECUTION BLOCK

config = {
    "source_experiment_name": {
        "dev": f"dummy_{ENV_SHORT_NAME}",
        "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    }
}
# Check if user data file exists, if not fetch from query
if os.path.exists("user_data.parquet"):
    print("Loading existing user data from parquet file...")
    user_data_df = pd.read_parquet("user_data.parquet")
else:
    print("Fetching user data from query...")
    user_data = get_data_from_sql_query("sql/user_data.sql")
    user_data_df = pd.DataFrame(
        user_data,
        columns=["user_id", "offer_subcategory_id", "total_count", "subcategory_ratio"],
    )
    # Save for future use
    user_data_df.to_parquet("user_data.parquet", index=False)

print("user_data:", user_data_df.head())
source_artifact_uri = get_model_from_mlflow(
    # experiment_name=config["source_experiment_name"][ENV_SHORT_NAME],
    experiment_name=config["source_experiment_name"]["prod"],
    run_id=None,
    artifact_uri=None,
)
print(f"Model artifact_uri: {source_artifact_uri}")
# logger.info(f"Model artifact_uri: {source_artifact_uri}")

# logger.info(f"Download model from {source_artifact_uri} trained model..."
# download_model(artifact_uri=source_artifact_uri)
print("Model downloaded.")
# logger.info("Model downloaded.")

# Before loading model, check what we have
print("Model directory contents:")
model_files = os.listdir(MODEL_BASE_PATH)
print("\n".join(model_files))

print("\nLoad Two Tower model...")

# Check available tags in the model
saved_model_path = os.path.join(os.getcwd(), MODEL_BASE_PATH)
print(f"Loading model from: {saved_model_path}")
loaded_model = tf.keras.models.load_model(saved_model_path)
print(f"Model loaded successfully from: {saved_model_path}")

# You can now use the loaded_model for predictions, evaluation, etc.
# For example, to print the model summary:
loaded_model.summary()
print("Two Tower model loaded.")

# get user and item embeddings
item_list = loaded_model.item_layer.layers[0].get_vocabulary()
item_weights = loaded_model.item_layer.layers[1].get_weights()[0].astype(np.float32)
user_list = loaded_model.user_layer.layers[0].get_vocabulary()
user_weights = loaded_model.user_layer.layers[1].get_weights()[0].astype(np.float32)
user_embedding_dict = {x: y for x, y in zip(user_list, user_weights)}
item_embedding_dict = {x: y for x, y in zip(item_list, item_weights)}
user_id_list = user_data_df["user_id"].unique().tolist()

true_user_id_subset = user_id_list[:10]  # Take a subset of 10 user IDs for testing


n_calls_per_user = 3  # Number of calls to make per user
true_user_predictions, true_latencies = process_users_endpoint_calls(
    true_user_id_subset, n_calls_per_user
)

# Create results DataFrame
results = []

# Analyze true user predictions
print("\n=== Analysis of True User Predictions ===")
true_metrics = analyze_predictions(
    true_user_predictions.predictions,
    true_latencies,
    user_embedding_dict,
    item_embedding_dict,
)
true_metrics["user_type"] = "true"
results.append(true_metrics)

# Process mock users
## create mock users to test
mock_users = [
    {
        "user_id": f"user_{i}",
        "offer_subcategory_id": f"subcategory_{i % 5}",
        "total_count": i * 10,
        "subcategory_ratio": i / 100.0,
    }
    for i in range(10, 20)
]
##create datagrame from mock users
mock_users_df = pd.DataFrame(mock_users)
# Convert mock users to a list of dictionaries for processing

mock_user_id_list = mock_users_df["user_id"].unique().tolist()
mock_user_id_subset = mock_user_id_list[
    :10
]  # Take a subset of 10 mock user IDs for testing

mock_user_predictions, mock_latencies = process_users_endpoint_calls(
    mock_user_id_subset, n_calls_per_user
)
# Analyze the predictions for mock users
print("\n=== Analysis of Mock User Predictions ===")
mock_metrics = analyze_predictions(
    mock_user_predictions.predictions,
    mock_latencies,
    user_embedding_dict,
    item_embedding_dict,
)
mock_metrics["user_type"] = "mock"
results.append(mock_metrics)

# Create comparison DataFrame
results_df = pd.DataFrame(results)
# Reorder columns to put user_type first
cols = ["user_type"] + [col for col in results_df.columns if col != "user_type"]
results_df = results_df[cols]

print("\n=== Comparison Report ===")
print(results_df.to_string(index=False))
