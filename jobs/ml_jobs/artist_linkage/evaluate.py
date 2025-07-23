import gcsfs
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import typer

from constants import (
    ARTIST_ID_KEY,
    ARTIST_NAME_KEY,
    ARTIST_TYPE_KEY,
    OFFER_CATEGORY_ID_KEY,
    OFFER_IS_SYNCHRONISED,
    PRODUCT_ID_KEY,
    WIKI_ID_KEY,
)
from utils.mlflow import (
    connect_remote_mlflow,
    get_mlflow_experiment,
)

METRICS_PER_DATASET_CSV_FILENAME = "metrics_per_dataset.csv"
METRICS_PER_DATASET_GRAPH_FILENAME = "metrics_per_dataset.png"
GLOBAL_METRICS_FILENAME = "global_metrics.csv"


WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC = "wiki_matched_weighted_by_bookings_perc"
WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC = "wiki_matched_weighted_by_product_perc"
WIKI_MATCHED_PERC = "wiki_matched_perc"

TOTAL_PRODUCT_COUNT_KEY = "total_product_count"
TOTAL_BOOKING_COUNT_KEY = "total_booking_count"
DATASET_NAME_KEY = "dataset_name"
RAW_ARTIST_NAME_KEY = "raw_artist_name"


### Params
def compute_metrics_per_dataset(
    artists_per_dataset: pd.DataFrame,
) -> pd.Series:
    """
    Compute classification metrics for a dataset of artists.
    This function calculates standard binary classification metrics (precision, recall, F1-score)
    based on true positives, false positives, false negatives, and true negatives columns
    in the input DataFrame.
    Args:
        artists_per_dataset (pd.DataFrame): DataFrame containing artist data with boolean columns
            'tp' (true positives), 'fp' (false positives), 'fn' (false negatives),
            and 'tn' (true negatives).
    Returns:
        pd.Series: Series containing the computed metrics:
            - tp: Number of true positives
            - fp: Number of false positives
            - fn: Number of false negatives
            - tn: Number of true negatives
            - precision: Precision score (tp / (tp + fp))
            - recall: Recall score (tp / (tp + fn))
            - f1: F1-score (harmonic mean of precision and recall)
    Note:
        If denominators are zero, the corresponding metrics are set to 0 to avoid
        division by zero errors.
    """

    tp = len(artists_per_dataset.loc[artists_per_dataset.tp])
    fp = len(artists_per_dataset.loc[artists_per_dataset.fp])
    fn = len(artists_per_dataset.loc[artists_per_dataset.fn])
    tn = len(artists_per_dataset.loc[artists_per_dataset.tn])

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


def get_main_artist_per_dataset(
    linked_products_on_test_sets_df: pd.DataFrame,
) -> dict:
    """
    Get the main artist (with highest product count) for each dataset.
    This function filters for records where 'is_my_artist' is True, then groups by
    dataset name and artist ID to count unique products per artist. For each dataset,
    it returns the artist with the highest number of products.
    Args:
        linked_products_on_test_sets_df (pd.DataFrame): DataFrame containing linked
            products data with columns including 'is_my_artist', 'dataset_name',
            artist ID, and product ID columns.
    Returns:
        dict: Dictionary mapping dataset names to their corresponding main artist IDs.
            The main artist is defined as the one with the highest number of unique
            products in that dataset.
    """

    return (
        linked_products_on_test_sets_df.loc[lambda df: df.is_my_artist]
        .groupby([DATASET_NAME_KEY, ARTIST_ID_KEY])
        .agg(product_count=(PRODUCT_ID_KEY, "nunique"))
        .reset_index()
        .sort_values([DATASET_NAME_KEY, "product_count"], ascending=[True, False])
        .drop_duplicates(subset=[DATASET_NAME_KEY], keep="first")
        .set_index(DATASET_NAME_KEY)
        .to_dict()[ARTIST_ID_KEY]
    )


def get_test_sets_df(test_set_dir: str) -> pd.DataFrame:
    fs = gcsfs.GCSFileSystem()
    GS_PREFIX = "gs://"
    PARQUET_EXTENSION = ".parquet"

    parquet_files = [
        GS_PREFIX + path
        for path in fs.glob(f"{test_set_dir}/**")
        if path.endswith(PARQUET_EXTENSION)
    ]

    return pd.concat(
        [
            pd.read_parquet(test_set).assign(source_file_path=test_set)
            for test_set in parquet_files
        ]
    )


def project_linked_artists_on_test_sets(
    linked_products_df: pd.DataFrame,
    test_sets_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Project linked artists onto test sets by merging dataframes on artist and category information.
    This function performs an inner join between linked products and test sets based on
    artist name, offer category ID, and artist type to find matching records.
    Args:
        linked_products_df (pd.DataFrame): DataFrame containing linked product information
            with columns including raw artist name, offer category ID, and artist type.
        test_sets_df (pd.DataFrame): DataFrame containing test set information
            with columns including artist name, offer category ID, and artist type.
    Returns:
        pd.DataFrame: Merged DataFrame containing only records that match between
            linked products and test sets on the specified join keys.
    """

    return linked_products_df.merge(
        test_sets_df,
        how="inner",
        left_on=[RAW_ARTIST_NAME_KEY, OFFER_CATEGORY_ID_KEY, ARTIST_TYPE_KEY],
        right_on=[
            ARTIST_NAME_KEY,
            OFFER_CATEGORY_ID_KEY,
            ARTIST_TYPE_KEY,
        ],
    )


def get_wiki_matching_metrics(artists_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Wikipedia matching metrics for artists data.
    This function computes various statistics about how well artists are matched
    to Wikipedia entries, both overall and grouped by offer category.
    Args:
        artists_df (pd.DataFrame): DataFrame containing artist data with columns:
            - wiki_id: Wikipedia ID (may contain NaN for unmatched artists)
            - total_booking_count: Number of bookings per artist
            - total_product_count: Number of products per artist
            - offer_category_id: Category identifier for grouping
    Returns:
        pd.DataFrame: DataFrame with matching metrics including:
            - WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC: Percentage of bookings for matched artists
            - WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC: Percentage of products for matched artists
            - WIKI_MATCHED_PERC: Percentage of artists with Wikipedia matches
            - artist_name_count: Total number of artists
            - total_product_count: Sum of all products
            - total_booking_count: Sum of all bookings
            - category: Category identifier (index converted to column)
        Rows include 'TOTAL' for overall metrics and individual offer category IDs.
    """

    def _get_stats_per_df(input_df: pd.DataFrame) -> dict:
        return {
            WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC: round(
                100
                * input_df.loc[lambda df: df.wiki_id.notna()][
                    TOTAL_BOOKING_COUNT_KEY
                ].sum()
                / input_df[TOTAL_BOOKING_COUNT_KEY].sum(),
                2,
            ),
            WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC: round(
                100
                * input_df.loc[lambda df: df.wiki_id.notna()][
                    TOTAL_PRODUCT_COUNT_KEY
                ].sum()
                / input_df[TOTAL_PRODUCT_COUNT_KEY].sum(),
                2,
            ),
            WIKI_MATCHED_PERC: round(
                100 * input_df.wiki_id.notna().sum() / len(input_df),
                2,
            ),
            "artist_name_count": len(input_df),
            TOTAL_PRODUCT_COUNT_KEY: input_df.total_product_count.sum(),
            TOTAL_BOOKING_COUNT_KEY: input_df.total_booking_count.sum(),
        }

    stats_dict = {"TOTAL": _get_stats_per_df(artists_df)}
    for group_name, group_df in artists_df.groupby("offer_category_id"):
        stats_dict[group_name] = _get_stats_per_df(group_df)

    return pd.DataFrame(stats_dict).T.assign(category=lambda df: df.index)


def get_matching_metrics_per_dataset(
    linked_products_on_test_sets_df: pd.DataFrame,
    main_artist_per_dataset: dict,
) -> pd.DataFrame:
    """
    Calculate matching metrics (precision, recall, F1) for each dataset based on artist clustering.

    This function evaluates the performance of artist clustering by comparing predicted main clusters
    with actual artist assignments across different datasets. It computes confusion matrix components
    (TP, FP, FN, TN) and derives classification metrics for each dataset.

    Args:
        linked_products_on_test_sets_df (pd.DataFrame): DataFrame containing test data with columns:
            - artist_id: Identifier for the artist
            - is_my_artist: Boolean indicating if the artist belongs to the expected category
            - dataset_name: Name of the dataset for grouping
        main_artist_per_dataset (dict): Dictionary mapping dataset names to main artist IDs
            that represent the primary cluster for each dataset

    Returns:
        pd.DataFrame: DataFrame with computed metrics for each dataset, including:
            - dataset_name: Name of the dataset
            - Additional metric columns as computed by compute_metrics_per_dataset function
            (typically precision, recall, F1-score, etc.)

    Note:
        The function uses the following logic for classification:
        - True Positive (tp): Artist is correctly identified as main cluster
        - False Positive (fp): Artist is incorrectly identified as main cluster
        - False Negative (fn): Artist should be main cluster but isn't identified as such
        - True Negative (tn): Artist is correctly not identified as main cluster
    """

    return (
        linked_products_on_test_sets_df.assign(
            is_main_cluster=lambda df: df.artist_id.isin(
                main_artist_per_dataset.values()
            ),
            tp=lambda df: df.is_my_artist & df.is_main_cluster,
            fp=lambda df: ~df.is_my_artist & df.is_main_cluster,
            fn=lambda df: df.is_my_artist & ~df.is_main_cluster,
            tn=lambda df: ~df.is_my_artist & ~df.is_main_cluster,
        )
        .groupby(DATASET_NAME_KEY)
        .apply(compute_metrics_per_dataset)
        .reset_index()
    )


app = typer.Typer()


@app.command()
def main(
    products_to_link_file_path: str = typer.Option(),
    artists_file_path: str = typer.Option(),
    product_artist_link_file_path: str = typer.Option(),
    test_sets_dir: str = typer.Option(),
    experiment_name: str = typer.Option(),
) -> None:
    # %% Load Data
    test_sets_df = get_test_sets_df(test_sets_dir).rename(
        columns={"is_synchronised": OFFER_IS_SYNCHRONISED}
    )
    products_to_link_df = (
        pd.read_parquet(products_to_link_file_path)
        .astype({PRODUCT_ID_KEY: int})
        .rename(columns={ARTIST_NAME_KEY: RAW_ARTIST_NAME_KEY})
    )
    artists_df = pd.read_parquet(artists_file_path)
    product_artist_link_df = pd.read_parquet(product_artist_link_file_path).astype(
        {PRODUCT_ID_KEY: int}
    )

    # %% Rebuild products with artists metadata
    linked_products_df = products_to_link_df.merge(
        product_artist_link_df,
        how="left",
        on=[PRODUCT_ID_KEY, ARTIST_TYPE_KEY],
    ).merge(artists_df, how="left", on=ARTIST_ID_KEY)

    # Global Metrics
    artists_with_stats_df = (
        linked_products_df.groupby([ARTIST_ID_KEY, OFFER_CATEGORY_ID_KEY])
        .agg(
            total_product_count=(PRODUCT_ID_KEY, "nunique"),
            total_booking_count=(TOTAL_BOOKING_COUNT_KEY, "sum"),
            artist_name=(ARTIST_NAME_KEY, "first"),
            wiki_id=(WIKI_ID_KEY, "first"),
        )
        .reset_index()
    )
    global_wiki_matching_metrics_df = get_wiki_matching_metrics(artists_with_stats_df)

    # Test Set Metrics
    linked_products_on_test_sets_df = project_linked_artists_on_test_sets(
        linked_products_df=linked_products_df, test_sets_df=test_sets_df
    )

    main_artist_per_dataset = get_main_artist_per_dataset(
        linked_products_on_test_sets_df
    )

    metrics_per_dataset_df = get_matching_metrics_per_dataset(
        linked_products_on_test_sets_df=linked_products_on_test_sets_df,
        main_artist_per_dataset=main_artist_per_dataset,
    )

    # MLflow Logging
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name=experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        # Log Dataset
        dataset = mlflow.data.from_pandas(
            linked_products_on_test_sets_df,
            name="linked_products_on_test_sets_df",
        )
        mlflow.log_input(dataset, context="evaluation")

        # Log Metrics
        metrics_per_dataset_df.to_csv(METRICS_PER_DATASET_CSV_FILENAME, index=False)
        global_wiki_matching_metrics_df.to_csv(GLOBAL_METRICS_FILENAME, index=False)
        mlflow.log_artifact(METRICS_PER_DATASET_CSV_FILENAME)
        mlflow.log_artifact(GLOBAL_METRICS_FILENAME)
        mlflow.log_metrics(
            {
                "precision_mean": metrics_per_dataset_df.precision.mean(),
                "precision_std": metrics_per_dataset_df.precision.std(),
                "recall_mean": metrics_per_dataset_df.recall.mean(),
                "recall_std": metrics_per_dataset_df.recall.std(),
                "f1_mean": metrics_per_dataset_df.f1.mean(),
                "f1_std": metrics_per_dataset_df.f1.std(),
                WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC: global_wiki_matching_metrics_df[
                    WIKI_MATCHED_WEIGHTED_BY_BOOKINGS_PERC
                ]["TOTAL"],
                WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC: global_wiki_matching_metrics_df[
                    WIKI_MATCHED_WEIGHTED_BY_PRODUCT_PERC
                ]["TOTAL"],
                WIKI_MATCHED_PERC: global_wiki_matching_metrics_df["wiki_matched_perc"][
                    "TOTAL"
                ],
            }
        )

        # Create and Log Graph
        ax = metrics_per_dataset_df.plot.barh(
            x=DATASET_NAME_KEY, y=["precision", "recall", "f1"], rot=0, figsize=(8, 12)
        )
        ax.legend(loc="upper left")
        plt.tight_layout()
        plt.savefig(METRICS_PER_DATASET_GRAPH_FILENAME)
        mlflow.log_artifact(METRICS_PER_DATASET_GRAPH_FILENAME)


if __name__ == "__main__":
    app()
