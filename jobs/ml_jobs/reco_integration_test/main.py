import typer

import pandas as pd
from loguru import logger
from utils.analysis_utils import analyze_predictions
from utils.endpoint import process_endpoint_calls
from utils.tools import (
    fetch_user_item_data_with_embeddings,
)


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

    call_type_results = []
    for call_type in analysis_config:
        logger.info(f"\n=== Processing {call_type} calls ===")
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
    logger.info(call_type_results_df.to_string(index=False))
    call_type_results_df.to_parquet(
        f"{config['storage_path']}/integration_tests_reports.parquet",
        index=False,
    )
    logger.info(
        f"\nComparison report saved to {config['storage_path']}"
        "/integration_tests_reports.parquet"
    )


if __name__ == "__main__":
    typer.run(main)
