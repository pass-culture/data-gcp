import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import typer

from utils.constants import ENV_SHORT_NAME, EXPERIMENT_BASE_NAME
from utils.mlflow import (
    connect_remote_mlflow,
    get_mlflow_client_id,
    get_mlflow_experiment,
)

METRICS_PER_DATASET_CSV_PATH = "metrics_per_dataset.csv"
METRICS_PER_DATASET_GRAPH_PATH = "metrics_per_dataset.png"

app = typer.Typer()


### Params
def compute_metrics_per_slice(
    artists_per_slice: pd.api.typing.DataFrameGroupBy,
) -> pd.Series:
    tp = artists_per_slice.loc[artists_per_slice.tp].offer_number.sum()
    fp = artists_per_slice.loc[artists_per_slice.fp].offer_number.sum()
    fn = artists_per_slice.loc[artists_per_slice.fn].offer_number.sum()
    tn = artists_per_slice.loc[artists_per_slice.tn].offer_number.sum()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = (
        2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    )

    return pd.Series(
        {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }
    )


def get_main_matched_cluster_per_dataset(
    matched_artists_in_test_set_df: pd.DataFrame,
) -> dict:
    return (
        (
            matched_artists_in_test_set_df.loc[lambda df: df.is_my_artist]
            .groupby(["dataset_name", "cluster_id"])
            .agg(
                {
                    "offer_number": "sum",
                }
            )
            .sort_values(["dataset_name", "offer_number"], ascending=[True, False])
        )
        .reset_index()
        .drop_duplicates(subset=["dataset_name"], keep="first")
        .drop(columns=["offer_number"])
        .set_index("dataset_name")
        .to_dict()["cluster_id"]
    )


@app.command()
def main(
    input_file_path: str = typer.Option(),
) -> None:
    matched_artists_in_test_set_df = pd.read_parquet(input_file_path)

    main_cluster_per_dataset = get_main_matched_cluster_per_dataset(
        matched_artists_in_test_set_df
    )

    metrics_per_dataset_df = (
        matched_artists_in_test_set_df.assign(
            is_main_cluster=lambda df: df.cluster_id.isin(
                main_cluster_per_dataset.values()
            ),
            tp=lambda df: df.is_my_artist & df.is_main_cluster,
            fp=lambda df: ~df.is_my_artist & df.is_main_cluster,
            fn=lambda df: df.is_my_artist & ~df.is_main_cluster,
            tn=lambda df: ~df.is_my_artist & ~df.is_main_cluster,
        )
        .groupby("dataset_name")
        .apply(compute_metrics_per_slice)
        .reset_index()
    )
    metrics_dict = {
        "precision_mean": metrics_per_dataset_df.precision.mean(),
        "precision_std": metrics_per_dataset_df.precision.std(),
        "recall_mean": metrics_per_dataset_df.recall.mean(),
        "recall_std": metrics_per_dataset_df.recall.std(),
        "f1_mean": metrics_per_dataset_df.f1.mean(),
        "f1_std": metrics_per_dataset_df.f1.std(),
    }

    # MLflow Logging
    connect_remote_mlflow(get_mlflow_client_id())
    experiment_name = f"{EXPERIMENT_BASE_NAME}_{ENV_SHORT_NAME}"
    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        # Log Dataset
        dataset = mlflow.data.from_pandas(
            matched_artists_in_test_set_df,
            source=input_file_path,
            name="artists_on_test_sets",
        )
        mlflow.log_input(dataset, context="evaluation")

        # Log Metrics
        metrics_per_dataset_df.to_csv(METRICS_PER_DATASET_CSV_PATH, index=False)
        mlflow.log_artifact(METRICS_PER_DATASET_CSV_PATH)
        mlflow.log_metrics(metrics_dict)

        # Create and Log Graph
        ax = metrics_per_dataset_df.plot.barh(
            x="dataset_name", y=["precision", "recall", "f1"], rot=0, figsize=(8, 12)
        )
        ax.legend(loc="upper left")
        plt.tight_layout()
        plt.savefig(METRICS_PER_DATASET_GRAPH_PATH)
        mlflow.log_artifact(METRICS_PER_DATASET_GRAPH_PATH)


if __name__ == "__main__":
    app()
