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
from utils.analysis_utils import analyze_predictions, _analyze_search_types

MODEL_BASE_PATH = "model"
RETRIEVAL_SIZE = 60  # Default size for retrieval, can be adjusted as needed


def process_users_endpoint_calls(user_id_subset, n_calls_per_user):
    """
    Call the recommendation endpoint for each user in the subset, multiple times.
    Returns predictions, latencies, and success/failure counts.
    """
    predictions_by_user = defaultdict(list)
    test_user = defaultdict(list)
    latencies = []
    success_count = 0
    failure_count = 0
    for user_id in user_id_subset:
        for call_num in range(n_calls_per_user):
            test_user = defaultdict(list)
            start_time = time.time()
            results = call_endpoint(
                model_type="recommendation",
                user_id=user_id,
                size=RETRIEVAL_SIZE,
            )
            end_time = time.time()
            latencies.append(end_time - start_time)
            predictions_by_user[user_id].append(results.predictions)
            if hasattr(results, "predictions") and results.predictions:
                success_count += 1
            else:
                failure_count += 1
            print(f"Call {call_num + 1} completed for user {user_id}")
            ### search type analysis can be added here if needed
    print(f"Total successful calls: {success_count}")
    print(f"Total failed calls: {failure_count}")
    return predictions_by_user, latencies, success_count, failure_count


def test_user_set(
    user_id_subset, user_type, config, user_embedding_dict, item_embedding_dict
):
    """
    Run endpoint calls and analysis for a given user subset.
    Returns a metrics dictionary.
    """
    print(f"\n=== Analysis of {user_type.capitalize()} User Predictions ===")
    predictions, latencies, success_count, failure_count = process_users_endpoint_calls(
        user_id_subset, config["number_of_calls_per_user"]
    )
    metrics = analyze_predictions(
        predictions,
        latencies,
        user_embedding_dict,
        item_embedding_dict,
        success_count=success_count,
        failure_count=failure_count,
    )
    metrics["user_type"] = user_type
    metrics["number_of_users"] = len(user_id_subset)
    return metrics


def main():
    # Configuration
    config = {
        "source_experiment_name": {
            "dev": f"dummy_{ENV_SHORT_NAME}",
            "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
            "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        },
        "number_of_users": 100,
        "number_of_calls_per_user": 1,
    }

    # Load user data
    if os.path.exists("user_data.parquet"):
        print("Loading existing user data from parquet file...")
        user_data_df = pd.read_parquet("user_data.parquet")
    else:
        print("Fetching user data from query...")
        user_data = get_data_from_sql_query("sql/user_data.sql")
        user_data_df = pd.DataFrame(
            user_data,
            columns=[
                "user_id",
                "offer_subcategory_id",
                "total_count",
                "subcategory_ratio",
            ],
        )
        user_data_df.to_parquet("user_data.parquet", index=False)

    print("user_data:", user_data_df.head())
    source_artifact_uri = get_model_from_mlflow(
        experiment_name=config["source_experiment_name"]["prod"],
        run_id=None,
        artifact_uri=None,
    )
    print(f"Model artifact_uri: {source_artifact_uri}")
    download_model(artifact_uri=source_artifact_uri)
    print("Model downloaded.")

    print("Model directory contents:")
    model_files = os.listdir(MODEL_BASE_PATH)
    print("\n".join(model_files))

    print("\nLoad Two Tower model...")
    saved_model_path = os.path.join(os.getcwd(), MODEL_BASE_PATH)
    print(f"Loading model from: {saved_model_path}")
    loaded_model = tf.keras.models.load_model(saved_model_path)
    print(f"Model loaded successfully from: {saved_model_path}")
    loaded_model.summary()
    print("Two Tower model loaded.")

    # Get user and item embeddings
    item_list = loaded_model.item_layer.layers[0].get_vocabulary()
    item_weights = loaded_model.item_layer.layers[1].get_weights()[0].astype(np.float32)
    user_list = loaded_model.user_layer.layers[0].get_vocabulary()
    user_weights = loaded_model.user_layer.layers[1].get_weights()[0].astype(np.float32)
    user_embedding_dict = dict(zip(user_list, user_weights, strict=True))
    item_embedding_dict = dict(zip(item_list, item_weights, strict=True))
    user_id_list = user_data_df["user_id"].unique().tolist()

    # print users size
    print(f"Number of users in user_data_df: {len(user_id_list)}")
    print(f"Number of users in model: {len(user_list)}")
    # print preview of user list
    print("User list preview:", user_list[:10])
    # Filter user IDs to those present in the model's user embedding vocabulary
    user_id_list = [uid for uid in user_id_list if uid in user_list]
    true_user_id_subset = user_id_list[: config["number_of_users"]]
    print("len(true_user_id_subset):", len(true_user_id_subset))

    results = []
    # Test real users
    true_metrics = test_user_set(
        true_user_id_subset, "true", config, user_embedding_dict, item_embedding_dict
    )
    results.append(true_metrics)

    # Test mock users
    mock_users = [
        {
            "user_id": f"user_{i}",
            "offer_subcategory_id": f"subcategory_{i % 5}",
            "total_count": i * 10,
            "subcategory_ratio": i / 100.0,
        }
        for i in range(10, 20)
    ]
    mock_users_df = pd.DataFrame(mock_users)
    mock_user_id_list = mock_users_df["user_id"].unique().tolist()
    mock_user_id_subset = mock_user_id_list[:10]
    mock_metrics = test_user_set(
        mock_user_id_subset, "mock", config, user_embedding_dict, item_embedding_dict
    )
    results.append(mock_metrics)

    # Create and save comparison DataFrame
    results_df = pd.DataFrame(results)
    cols = ["user_type"] + [col for col in results_df.columns if col != "user_type"]
    results_df = results_df[cols]

    print("\n=== Comparison Report ===")
    print(results_df.to_string(index=False))
    results_df.to_csv("comparison_report.csv", index=False)
    print("\nComparison report saved to comparison_report.csv")


if __name__ == "__main__":
    main()
