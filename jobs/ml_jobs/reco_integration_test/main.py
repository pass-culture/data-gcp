import time
from collections import defaultdict

import pandas as pd

from utils.endpoint import call_endpoint
from constants import ENV_SHORT_NAME
from utils.analysis_utils import analyze_predictions
from utils.tools import (
    fetch_user_item_data_with_embeddings,
)

RETRIEVAL_SIZE = 60  # Default size for retrieval, can be adjusted as needed


def process_endpoint_calls(call_type, ids, n_calls_per_user):
    """
    Call the recommendation endpoint for each user in the subset, multiple times.
    Returns predictions, latencies, and success/failure counts.
    """
    predictions_by_id = defaultdict(list)
    latencies = []
    success_count = 0
    failure_count = 0
    for id in ids:
        for call_num in range(n_calls_per_user):
            start_time = time.time()
            results = call_endpoint(
                model_type=call_type,
                id=id,
                size=RETRIEVAL_SIZE,
            )
            end_time = time.time()
            latencies.append(end_time - start_time)
            predictions_by_id[id].append(results.predictions)
            if hasattr(results, "predictions") and results.predictions:
                success_count += 1
            else:
                failure_count += 1
            print(f"Call {call_num + 1} completed for user {id}")
            ### search type analysis can be added here if needed
    print(f"Total successful calls: {success_count}")
    print(f"Total failed calls: {failure_count}")
    return predictions_by_id, latencies, success_count, failure_count


def main():
    # Configuration
    config = {
        "source_experiment_name": {
            "dev": f"dummy_{ENV_SHORT_NAME}",
            "stg": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
            "prod": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
        },
        "number_of_ids": 100,
        "number_of_mock_ids": 10,
        "number_of_calls_per_user": 2,
    }

    # Load user data
    (
        user_id_list,
        item_id_list,
        mock_user_id_list,
        mock_item_id_list,
        user_embedding_dict,
        item_embedding_dict,
    ) = fetch_user_item_data_with_embeddings(config)
    print("len(item_id_list):", len(item_id_list))

    analysis_config = {
        "recommendation": {
            "true_ids": user_id_list,
            "mock_ids": mock_user_id_list,
        },
        "similar_offer": {
            "true_ids": item_id_list,
            "mock_ids": mock_item_id_list,
        },
    }
    results = {}
    for call_type in analysis_config:
        print(f"\n=== Processing {call_type} calls ===")
        call_type_results = []
        for data_type in ["true", "mock"]:
            print(f"\nProcessing {data_type} IDs for {call_type} calls...")
            # Test real users
            ids = analysis_config[call_type][f"{data_type}_ids"]

            predictions, latencies, success_count, failure_count = (
                process_endpoint_calls(
                    call_type, ids, config["number_of_calls_per_user"]
                )
            )
            print(f"Processed {len(ids)} {data_type} IDs for {call_type} calls.")
            # Analyze predictions
            print(
                f"Analyzing predictions for {call_type} calls with {data_type} IDs..."
            )
            # Flatten predictions for analysis
            metrics = analyze_predictions(
                predictions,
                latencies,
                user_embedding_dict,
                item_embedding_dict,
                success_count=success_count,
                failure_count=failure_count,
            )
            call_type_results.append(metrics)

        # Create and save comparison DataFrame
        call_type_results_df = pd.DataFrame(call_type_results)
        results[call_type] = call_type_results_df

        print("\n=== Comparison Report ===")
        print(call_type_results_df.to_string(index=False))
        call_type_results_df.to_csv(f"{call_type}_comparison_report.csv", index=False)
        print(f"\nComparison report saved to {call_type}_comparison_report.csv")


if __name__ == "__main__":
    main()
