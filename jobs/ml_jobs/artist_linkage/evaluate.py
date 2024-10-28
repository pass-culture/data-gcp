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
GLOBAL_METRICS_FILENAME = "global_metrics.csv"

MERGE_COLUMNS = ["artist_name", "offer_category_id", "is_synchronised", "artist_type"]

WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC = "wiki_matched_weighted_by_bookings_perc"
WIKI_MATCHED_WEIGHTED_BY_OFFERS_PERC = "wiki_matched_weighted_by_offers_perc"
WIKI_MATCHED_PERC = "wiki_matched_perc"

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
        for path in fs.glob(f"{test_set_dir}/**")
        if path.endswith(".parquet")
    ]

    return pd.concat(
        [
            pd.read_parquet(test_set).assign(source_file_path=test_set)
            for test_set in parquet_files
        ]
    )


def project_linked_artists_on_test_sets(
    artists_to_link_df: pd.DataFrame,
    linked_artists_df: pd.DataFrame,
    test_sets_df: pd.DataFrame,
) -> pd.DataFrame:
    return (
        test_sets_df.loc[
            :,
            MERGE_COLUMNS
            + [
                "dataset_name",
                "is_my_artist",
                "irrelevant_data",
            ],
        ]
        .merge(
            artists_to_link_df.loc[
                :,
                MERGE_COLUMNS
                + [
                    "offer_number",
                    "total_booking_count",
                ],
            ],
            how="left",
            on=MERGE_COLUMNS,
        )
        .merge(
            linked_artists_df.loc[:, MERGE_COLUMNS + ["cluster_id", "first_artist"]],
            how="left",
            on=MERGE_COLUMNS,
        )
    ).sort_values(by=["dataset_name", "cluster_id"])


def get_wiki_matching_metrics(artists_df: pd.DataFrame) -> pd.DataFrame:
    stats_dict = {}

    def _get_stats_per_df(input_df: pd.DataFrame) -> dict:
        return {
            WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC: round(
                100
                * input_df.loc[lambda df: df.wiki_id.notna()].total_booking_count.sum()
                / input_df.total_booking_count.sum(),
                2,
            ),
            WIKI_MATCHED_WEIGHTED_BY_OFFERS_PERC: round(
                100
                * input_df.loc[lambda df: df.wiki_id.notna()]
                .offer_number.replace(0, 1)
                .sum()
                / input_df.offer_number.replace(0, 1).sum(),
                2,
            ),  # TODO: Remove the replace(0, 1) when the offer_number is fixed
            WIKI_MATCHED_PERC: round(
                100 * input_df.wiki_id.notna().sum() / len(input_df),
                2,
            ),
            "artist_name_count": len(input_df),
            "total_booking_count": input_df.total_booking_count.sum(),
        }

    stats_dict["TOTAL"] = _get_stats_per_df(artists_df)
    for group_name, group_df in artists_df.groupby("offer_category_id"):
        stats_dict[group_name] = _get_stats_per_df(group_df)

    return pd.DataFrame(stats_dict).T.assign(category=lambda df: df.index)


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

    # Global Metrics
    wiki_matching_metrics_df = get_wiki_matching_metrics(linked_artists_df)

    # Test Set Metrics
    matched_artists_in_test_set_df = project_linked_artists_on_test_sets(
        artists_to_link_df=artists_to_link_df,
        linked_artists_df=linked_artists_df,
        test_sets_df=test_sets_df,
    )

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
        WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC: wiki_matching_metrics_df[
            WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC
        ]["TOTAL"],
        WIKI_MATCHED_WEIGHTED_BY_OFFERS_PERC: wiki_matching_metrics_df[
            WIKI_MATCHED_WEIGHTED_BY_OFFERS_PERC
        ]["TOTAL"],
        WIKI_MATCHED_PERC: wiki_matching_metrics_df["wiki_matched_perc"]["TOTAL"],
    }

    # MLflow Logging
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name=experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        # Log Dataset
        dataset = mlflow.data.from_pandas(
            matched_artists_in_test_set_df,
            name="artists_on_test_sets",
        )
        mlflow.log_input(dataset, context="evaluation")

        # Log Metrics
        metrics_per_dataset_df.to_csv(METRICS_PER_DATASET_CSV_FILENAME, index=False)
        wiki_matching_metrics_df.to_csv(GLOBAL_METRICS_FILENAME, index=False)
        mlflow.log_artifact(METRICS_PER_DATASET_CSV_FILENAME)
        mlflow.log_artifact(GLOBAL_METRICS_FILENAME)
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
