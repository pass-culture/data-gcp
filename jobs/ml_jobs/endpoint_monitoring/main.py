import pandas as pd
import typer
from loguru import logger

from utils.analysis_utils import analyze_predictions
from utils.endpoint import process_endpoint_calls
from utils.tools import (
    fetch_user_item_data_with_embeddings,
)


def process_call_type_data(config, call_type, data_type, ids):
    logger.info(f"\nProcessing {data_type} IDs for {call_type} calls...")
    predictions, latencies, success_count, failure_count = process_endpoint_calls(
        endpoint_name=config["endpoint_name"],
        call_type=call_type,
        ids=ids,
        n_calls_per_user=config["number_of_calls_per_user"],
    )
    return predictions, latencies, success_count, failure_count


def main(
    endpoint_name: str = typer.Option(..., help="Name of the endpoint"),
    experiment_name: str = typer.Option(..., help="Name of the experiment"),
    storage_path: str = typer.Option(..., help="Path to the data"),
    number_of_ids: int = typer.Option(10, help="Number of real IDs to test"),
    number_of_mock_ids: int = typer.Option(10, help="Number of mock IDs to test"),
    number_of_calls_per_user: int = typer.Option(2, help="Number of calls per user"),
):
    """
    Runs monitoring tests for recommendation endpoints.

    Parameters:
        endpoint_name (str): Name of the endpoint to test.
        experiment_name (str): Name of the experiment for tracking and configuration.
        storage_path (str): Path where results and reports will be stored.
        number_of_ids (int): Number of real user IDs to test.
        number_of_mock_ids (int): Number of mock user IDs to test.
        number_of_calls_per_user (int): Number of endpoint calls per user/item.

    Process:
        - Loads user and item data along with their embeddings.
        - For each call type processes both real and mock IDs.
        - Collects predictions for each scenario.
        - Analyzes prediction results and aggregates metrics.
        - Saves a comparison report as a parquet file in the specified storage path.

    Outputs:
        - Logs detailed metrics and saves a DataFrame report of the results.
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

    call_type_results = []
    for call_type, ids_dict in analysis_config.items():
        logger.info(f"\n=== Processing {call_type} calls ===")
        for data_type in ["true", "mock"]:
            ids = ids_dict[f"{data_type}_ids"]
            predictions, latencies, success_count, failure_count = (
                process_call_type_data(config, call_type, data_type, ids)
            )
            metrics = {}
            metrics.update(
                {
                    "date": pd.Timestamp.today().normalize(),
                    "call_type": call_type,
                    "data_type": data_type,
                }
            )
            metrics = analyze_predictions(
                metrics,
                predictions,
                user_embedding_dict,
                item_embedding_dict,
                latencies,
                success_count=success_count,
                failure_count=failure_count,
            )
            call_type_results.append(metrics)
    call_type_results_df = pd.DataFrame(call_type_results)
    logger.info(call_type_results_df.to_string(index=False))
    call_type_results_df.to_parquet(
        f"{config['storage_path']}/endpoint_monitoring_reports.parquet",
        index=False,
    )
    logger.info(
        f"\nComparison report saved to {config['storage_path']}"
        "/endpoint_monitoring_reports.parquet"
    )


if __name__ == "__main__":
    typer.run(main)
