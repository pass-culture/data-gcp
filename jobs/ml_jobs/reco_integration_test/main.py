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
    get_user_data_from_query,
)
from utils.model_utils import load_model, extract_embeddings
from utils.analysis_utils import analyze_predictions

MODEL_BASE_PATH = "./model"


def process_user_predictions(user_id_subset: list[str], n_calls_per_user: int):
    """Make multiple calls per user to analyze consistency."""
    """Analyze predictions and return metrics dictionary for reporting."""
    metrics = {}
    print("\n=== Analysis of Recommendations ===")

    # 1. Average number of recommendations per call
    reco_counts = [
        len(preds)
        for user_preds in predictions_by_user.values()
        for preds in user_preds
    ]
    avg_recos = np.mean(reco_counts)
    metrics["avg_recommendations"] = avg_recos
    print(f"Average number of recommendations per call: {avg_recos:.2f}")

    # 2. Response format example
    sample_pred = next(iter(predictions_by_user.values()))[0][0]
    # Convert MapComposite to dictionary before JSON serialization
    sample_pred_dict = convert_to_dict(sample_pred)
    print(json.dumps(sample_pred_dict, indent=2))

    # Response code analysis
    response_code_counts = {"success": 0, "error": 0}
    total_responses = 0
    for user_preds in predictions_by_user.values():
        for preds in user_preds:
            total_responses += 1
            if preds:
                pred_dict = convert_to_dict(preds[0])
                is_error = any(
                    "error" in str(value).lower() for value in pred_dict.values()
                )
                response_code_counts["error" if is_error else "success"] += 1
            else:
                response_code_counts["error"] += 1

    success_rate = (
        response_code_counts["success"] / total_responses * 100
        if total_responses
        else 0
    )
    metrics["success_rate"] = success_rate
    print("\nResponse code distribution:")
    for code, count in response_code_counts.items():
        percentage = (count / total_responses) * 100
        print(f"- {code}: {count} ({percentage:.2f}%)")

    # Search type distribution
    search_type_counts = defaultdict(int)
    total_predictions = 0
    for user_preds in predictions_by_user.values():
        for preds in user_preds:
            for pred in preds:
                pred_dict = convert_to_dict(pred)
                search_type = pred_dict.get("_search_type", "unknown")
                search_type_counts[search_type] += 1
                total_predictions += 1

    metrics["search_types"] = dict(search_type_counts)
    print("\nSearch type distribution:")
    for search_type, count in sorted(search_type_counts.items()):
        percentage = (count / total_predictions) * 100
        metrics[f"search_type_{search_type}"] = percentage
        print(f"- {search_type}: {count} ({percentage:.2f}%)")

    # Overlap analysis
    avg_user_overlap = 0
    overlap_count = 0
    for user_id, user_preds in predictions_by_user.items():
        if len(user_preds) >= 2:
            set1 = {str(convert_to_dict(item)) for item in user_preds[0]}
            set2 = {str(convert_to_dict(item)) for item in user_preds[1]}
            overlap = len(set1.intersection(set2))
            overlap_percentage = (overlap / len(set1)) * 100 if set1 else 0
            avg_user_overlap += overlap_percentage
            overlap_count += 1

    metrics["avg_user_overlap"] = (
        avg_user_overlap / overlap_count if overlap_count > 0 else 0
    )

    # Different users diversity
    all_user_recs = {
        user_id: {str(convert_to_dict(item)) for preds in user_preds for item in preds}
        for user_id, user_preds in predictions_by_user.items()
    }

    if len(all_user_recs) >= 2:
        user_pairs = [
            (u1, u2)
            for i, u1 in enumerate(all_user_recs.keys())
            for u2 in list(all_user_recs.keys())[i + 1 :]
        ]

        total_overlap = 0
        for u1, u2 in user_pairs:
            overlap = len(all_user_recs[u1].intersection(all_user_recs[u2]))
            total_items = len(all_user_recs[u1].union(all_user_recs[u2]))
            overlap_percentage = (overlap / total_items) * 100 if total_items else 0
            total_overlap += overlap_percentage

        avg_overlap = total_overlap / len(user_pairs) if user_pairs else 0
        metrics["avg_user_diversity"] = (
            100 - avg_overlap
        )  # Convert overlap to diversity

    # Latency analysis
    metrics["avg_latency"] = np.mean(latencies)
    metrics["min_latency"] = np.min(latencies)
    metrics["max_latency"] = np.max(latencies)
    metrics["p95_latency"] = np.percentile(latencies, 95)

    print("\nLatency analysis:")
    print(f"Average latency: {metrics['avg_latency']:.3f} seconds")
    print(f"Min latency: {metrics['min_latency']:.3f} seconds")
    print(f"Max latency: {metrics['max_latency']:.3f} seconds")
    print(f"95th percentile latency: {metrics['p95_latency']:.3f} seconds")

    # New: Item intralist similarity analysis
    if item_embedding_dict:
        print("\nItem Intralist Similarity Analysis:")
        avg_intralist_similarities = []
        for user_preds in predictions_by_user.values():
            for preds in user_preds:
                pred_dicts = [convert_to_dict(pred) for pred in preds]
                # Get item IDs from predictions
                item_ids = [
                    pred.get("item_id")
                    for pred in pred_dicts
                    if pred.get("item_id") in item_embedding_dict
                ]

                if len(item_ids) >= 2:
                    # Compute pairwise similarities
                    similarities = []
                    for i in range(len(item_ids)):
                        for j in range(i + 1, len(item_ids)):
                            sim = compute_cosine_similarity(
                                item_embedding_dict[item_ids[i]],
                                item_embedding_dict[item_ids[j]],
                            )
                            similarities.append(sim)
                    if similarities:
                        avg_intralist_similarities.append(np.mean(similarities))

        if avg_intralist_similarities:
            avg_intralist_sim = np.mean(avg_intralist_similarities)
            metrics["avg_intralist_similarity"] = avg_intralist_sim
            print(f"Average intralist item similarity: {avg_intralist_sim:.3f}")

    # New: User-Item similarity analysis
    if user_embedding_dict and item_embedding_dict:
        print("\nUser-Item Similarity Analysis:")
        user_item_similarities = []
        for user_id, user_preds in predictions_by_user.items():
            if user_id in user_embedding_dict:
                user_embedding = user_embedding_dict[user_id]
                for preds in user_preds:
                    pred_dicts = [convert_to_dict(pred) for pred in preds]
                    item_ids = [
                        pred.get("item_id")
                        for pred in pred_dicts
                        if pred.get("item_id") in item_embedding_dict
                    ]

                    for item_id in item_ids:
                        sim = compute_cosine_similarity(
                            user_embedding, item_embedding_dict[item_id]
                        )
                        user_item_similarities.append(sim)

        if user_item_similarities:
            avg_user_item_sim = np.mean(user_item_similarities)
            metrics["avg_user_item_similarity"] = avg_user_item_sim
            print(f"Average user-item similarity: {avg_user_item_sim:.3f}")

            p90_user_item_sim = np.percentile(user_item_similarities, 90)
            metrics["p90_user_item_similarity"] = p90_user_item_sim
            print(f"90th percentile user-item similarity: {p90_user_item_sim:.3f}")

    return metrics


# Make multiple calls per user to analyze consistency
def process_user_predictions(user_id_subset, n_calls_per_user):
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
            # print(f"Call {call_num + 1} completed")
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
    user_data = get_user_data_from_query()
    user_data_df = pd.DataFrame(
        user_data,
        columns=["user_id", "offer_subcategory_id", "total_count", "subcategory_ratio"],
    )
    # Save for future use
    user_data_df.to_parquet("user_data.parquet", index=False)

print("user_data:", user_data_df.head())
source_artifact_uri = get_model_from_mlflow(
    experiment_name=config["source_experiment_name"][ENV_SHORT_NAME],
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

# Try to load model directly first
try:
    print("Attempting to load model directly...")
    tf_reco = tf.keras.models.load_model(MODEL_BASE_PATH)
    print("Successfully loaded model using Keras load_model")
except Exception as e1:
    print(f"Direct loading failed: {e1}")
    try:
        print("Attempting to load as SavedModel without tags...")
        tf_reco = tf.saved_model.load(MODEL_BASE_PATH)
        print("Successfully loaded model using SavedModel without tags")
    except Exception as e2:
        print(f"SavedModel loading without tags failed: {e2}")
        try:
            print("Attempting to convert to TF 2.x SavedModel format...")
            # Try to convert to TF 2.x format
            converter = tf.compat.v1.wrap_function(
                lambda x: tf_reco(x),
                [tf.TensorSpec(shape=[None, None], dtype=tf.float32)],
            )
            tf.saved_model.save(converter, f"{MODEL_BASE_PATH}_converted")
            tf_reco = tf.keras.models.load_model(f"{MODEL_BASE_PATH}_converted")
            print("Successfully converted and loaded model")
        except Exception as e3:
            print(f"All loading attempts failed: {e3}")
            raise RuntimeError("Could not load the model in any format")

print("Two Tower model loaded.")

# Extract embeddings from the model using multiple approaches
print("Attempting to extract embeddings...")
try:
    # First attempt: Standard Keras layer approach
    user_list = tf_reco.user_layer.layers[0].get_vocabulary()
    user_weights = tf_reco.user_layer.layers[1].get_weights()[0].astype(np.float32)
    item_list = tf_reco.item_layer.layers[0].get_vocabulary()
    item_weights = tf_reco.item_layer.layers[1].get_weights()[0].astype(np.float32)
    print("Successfully extracted embeddings using standard layer approach")
except AttributeError:
    try:
        # Second attempt: Direct variable access
        print("Attempting direct variable access...")
        all_variables = [var.numpy() for var in tf_reco.variables]
        # Print available variables for debugging
        print(f"Found {len(all_variables)} variables in the model")
        for i, var in enumerate(all_variables):
            print(f"Variable {i} shape: {var.shape}")

        # Assume first set of variables are embeddings
        user_list = np.arange(all_variables[0].shape[0]).astype(str)
        user_weights = all_variables[0].astype(np.float32)
        item_list = np.arange(all_variables[2].shape[0]).astype(str)
        item_weights = all_variables[2].astype(np.float32)
        print("Successfully extracted embeddings using variable access")
    except Exception as e:
        print(f"Failed to extract embeddings: {e}")
        # Create dummy embeddings if everything fails
        print("Creating dummy embeddings...")
        dummy_dim = 32  # or whatever dimension your embeddings should be
        user_list = user_data_df["user_id"].unique()
        user_weights = np.random.normal(size=(len(user_list), dummy_dim)).astype(
            np.float32
        )
        item_list = np.array(["dummy_item"])
        item_weights = np.random.normal(size=(1, dummy_dim)).astype(np.float32)

# Create embedding dictionaries with explicit types for better code clarity
user_embedding_dict: dict[str, np.ndarray] = dict(
    zip(user_list, user_weights, strict=False)
)
item_embedding_dict: dict[str, np.ndarray] = dict(
    zip(item_list, item_weights, strict=False)
)
user_id_list = user_data_df["user_id"].unique().tolist()

true_user_id_subset = user_id_list[:10]  # Take a subset of 10 user IDs for testing


n_calls_per_user = 3  # Number of calls to make per user
true_user_predictions, true_latencies = process_user_predictions(
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

mock_user_predictions, mock_latencies = process_user_predictions(
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
