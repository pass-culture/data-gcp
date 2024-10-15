import gcsfs
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import typer

from utils.mlflow import (
    connect_remote_mlflow,
    get_mlflow_experiment,
)

METRICS_PER_DATASET_CSV_FILENAME = "metrics_per_dataset.csv"
METRICS_PER_DATASET_GRAPH_FILENAME = "metrics_per_dataset.png"

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


def get_test_sets_df(test_set_dir: str) -> pd.DataFrame:
    fs = gcsfs.GCSFileSystem()
    GS_PREFIX = "gs://"

    parquet_files = [
        GS_PREFIX + path
        for path in fs.glob(f"{GS_PREFIX}{test_set_dir}/**")
        if path.endswith(".parquet")
    ]

    return pd.concat(
        [
            pd.read_parquet(test_set).assign(source_file_path=test_set)
            for test_set in parquet_files
        ]
    )


@app.command()
def main(
    artists_to_link_file_path: str = typer.Option(),
    linked_artists_file_path: str = typer.Option(),
    test_sets_dir: str = typer.Option(),
    experiment_name: str = typer.Option(),
) -> None:
    test_sets_df = get_test_sets_df(test_sets_dir)
    artists_to_link_df = pd.read_parquet(artists_to_link_file_path)
    linked_artists_df = pd.read_parquet(linked_artists_file_path)

    matched_artists_in_test_set_df = (
        test_sets_df.loc[
            :,
            [
                "dataset_name",
                "is_my_artist",
                "irrelevant_data",
            ],
        ]
        .merge(
            artists_to_link_df.loc[
                :,
                [
                    "artist_name",
                    "offer_category_id",
                    "is_synchronised",
                    "artist_type",
                    "offer_number",
                    "total_booking_count",
                ],
            ],
            how="left",
            on=["artist_name", "offer_category_id", "is_synchronised", "artist_type"],
        )
        .merge(
            linked_artists_df.loc[:, ["cluster_id", "first_artist"]],
            how="left",
            on=["artist_name", "offer_category_id", "is_synchronised", "artist_type"],
        )
    ).sort_values(by=["dataset_name", "cluster_id"])

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
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name=experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        # Log Dataset
        dataset = mlflow.data.from_pandas(
            matched_artists_in_test_set_df,
            # source=input_file_path,
            name="artists_on_test_sets",
        )
        mlflow.log_input(dataset, context="evaluation")

        # Log Metrics
        metrics_per_dataset_df.to_csv(METRICS_PER_DATASET_CSV_FILENAME, index=False)
        mlflow.log_artifact(METRICS_PER_DATASET_CSV_FILENAME)
        mlflow.log_metrics(metrics_dict)

        # Create and Log Graph
        ax = metrics_per_dataset_df.plot.barh(
            x="dataset_name", y=["precision", "recall", "f1"], rot=0, figsize=(8, 12)
        )
        ax.legend(loc="upper left")
        plt.tight_layout()
        plt.savefig(METRICS_PER_DATASET_GRAPH_FILENAME)
        mlflow.log_artifact(METRICS_PER_DATASET_GRAPH_FILENAME)


if __name__ == "__main__":
    app()
