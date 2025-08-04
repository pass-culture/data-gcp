import time
from collections import defaultdict

import pandas as pd

from constants import ENV_SHORT_NAME, GCP_PROJECT, LOCATION
from utils.analysis_utils import analyze_predictions
from utils.endpoint import call_endpoint, get_endpoint_details
from utils.tools import (
    fetch_user_item_data_with_embeddings,
)

RETRIEVAL_SIZE = 60  # Default size for retrieval, can be adjusted as needed


def process_endpoint_calls(endpoint_path, call_type, ids, n_calls_per_user):
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
                endpoint_path=endpoint_path,
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


def main(
    endpoint_name: str,
    experiment_name: str,
    data_path: str,
    number_of_ids: int,
    number_of_mock_ids: int,
    number_of_calls_per_user: int,
):
    # Configuration
    config = {
        "endpoint_name": endpoint_name,
        "experiment_name": experiment_name,
        "data_path": data_path,
        "source_experiment_name": experiment_name,
        "number_of_ids": number_of_ids,
        "number_of_mock_ids": number_of_mock_ids,
        "number_of_calls_per_user": number_of_calls_per_user,
    }
    # config = {
    #     "endpoint_name": "recommendation_user_retrieval_prod",
    #     "source_experiment_name": f"algo_training_two_towers_v1.2_{ENV_SHORT_NAME}",
    #     "data_path": "data",
    #     "number_of_ids": 10,
    #     "number_of_mock_ids": 10,
    #     "number_of_calls_per_user": 2,
    # }

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
    # get endpont details
    endpoint_details = get_endpoint_details(
        config["endpoint_name"], gcp_project=GCP_PROJECT, location=LOCATION
    )
    for call_type in analysis_config:
        print(f"\n=== Processing {call_type} calls ===")
        call_type_results = []
        for data_type in ["true", "mock"]:
            print(f"\nProcessing {data_type} IDs for {call_type} calls...")
            # Test real users
            ids = analysis_config[call_type][f"{data_type}_ids"]

            predictions, latencies, success_count, failure_count = (
                process_endpoint_calls(
                    endpoint_path=endpoint_details["endpoint_path"],
                    call_type=call_type,
                    ids=ids,
                    n_calls_per_user=config["number_of_calls_per_user"],
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

        print(f"\n=== Comparison Report for {call_type} ===")
        print(call_type_results_df.to_string(index=False))
        call_type_results_df.to_csv(
            f"{data_type}_{call_type}_comparison_report.csv", index=False
        )
        # Concatenate all comparison CSVs into one file after processing all call_types
        if len(results) == len(analysis_config):
            csv_files = [
                f"{data_type}_{call_type}_comparison_report.csv"
                for call_type in analysis_config
                for data_type in ["true", "mock"]
            ]
            combined_df = pd.concat(
                [pd.read_csv(f) for f in csv_files], ignore_index=True
            )
            combined_df.to_csv("comparison_reports.csv", index=False)
            print("\nOverall comparison report saved to comparison_reports.csv")
        print(
            f"\nComparison report saved to {data_type}_{call_type}_comparison_report.csv"
        )


if __name__ == "__main__":
    main()
