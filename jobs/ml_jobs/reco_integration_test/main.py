import time
from collections import defaultdict
import typer

import pandas as pd
from loguru import logger
from constants import ENV_SHORT_NAME, GCP_PROJECT, LOCATION
from utils.analysis_utils import analyze_predictions
from utils.endpoint import call_endpoint, get_endpoint_path
from utils.tools import (
    fetch_user_item_data_with_embeddings,
)

RETRIEVAL_SIZE = 60  # Default size for retrieval, can be adjusted as needed


def process_endpoint_calls(endpoint_name, call_type, ids, n_calls_per_user):
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
                endpoint_path=get_endpoint_path(endpoint_name=endpoint_name),
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
            logger.info(f"Call {call_num + 1} completed for user {id}")
            ### search type analysis can be added here if needed
    logger.info(f"Total successful calls: {success_count}")
    logger.info(f"Total failed calls: {failure_count}")
    return predictions_by_id, latencies, success_count, failure_count


def main(
    endpoint_name: str = typer.Option(..., help="Name of the endpoint"),
    experiment_name: str = typer.Option(..., help="Name of the experiment"),
    storage_path: str = typer.Option(..., help="Path to the data"),
    number_of_ids: int = typer.Option(10, help="Number of real IDs to test"),
    number_of_mock_ids: int = typer.Option(10, help="Number of mock IDs to test"),
    number_of_calls_per_user: int = typer.Option(2, help="Number of calls per user"),
):
    """
    Run integration tests for recommendation endpoints using Typer CLI.
    """
    config = {
        "endpoint_name": endpoint_name,
        "experiment_name": experiment_name,
        "storage_path": storage_path,
        "source_experiment_name": experiment_name,
        "number_of_ids": number_of_ids,
        "number_of_mock_ids": number_of_mock_ids,
        "number_of_calls_per_user": number_of_calls_per_user,
    }

    (
        user_id_list,
        item_id_list,
        mock_user_id_list,
        mock_item_id_list,
        user_embedding_dict,
        item_embedding_dict,
    ) = fetch_user_item_data_with_embeddings(config)
    logger.info(f"len(item_id_list): {len(item_id_list)}")

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
        logger.info(f"\n=== Processing {call_type} calls ===")
        call_type_results = []
        for data_type in ["true", "mock"]:
            logger.info(f"\nProcessing {data_type} IDs for {call_type} calls...")
            ids = analysis_config[call_type][f"{data_type}_ids"]

            predictions, latencies, success_count, failure_count = (
                process_endpoint_calls(
                    endpoint_name=config["endpoint_name"],
                    call_type=call_type,
                    ids=ids,
                    n_calls_per_user=config["number_of_calls_per_user"],
                )
            )
            logger.info(f"Processed {len(ids)} {data_type} IDs for {call_type} calls.")
            metrics = {"call_type": call_type, "data_type": data_type}
            metrics.update(
                analyze_predictions(
                    predictions,
                    user_embedding_dict,
                    item_embedding_dict,
                    latencies,
                    success_count=success_count,
                    failure_count=failure_count,
                )
            )
            call_type_results.append(metrics)

        call_type_results_df = pd.DataFrame(call_type_results)
        results[call_type] = call_type_results_df

        logger.info(f"\n=== Comparison Report for {call_type} ===")
        logger.info(call_type_results_df.to_string(index=False))
        call_type_results_df.to_csv(f"{call_type}_comparison_report.csv", index=False)
        if len(results) == len(analysis_config):
            csv_files = [
                f"{call_type}_comparison_report.csv" for call_type in analysis_config
            ]
            combined_df = pd.concat(
                [pd.read_csv(f) for f in csv_files], ignore_index=True
            )
            combined_df.to_parquet(
                f"{config['storage_path']}/integration_tests_reports.parquet",
                index=False,
            )
        logger.info(
            f"\nComparison report saved to {config['storage_path']}"
            "/integration_tests_reports.parquet"
        )


if __name__ == "__main__":
    typer.run(main)
