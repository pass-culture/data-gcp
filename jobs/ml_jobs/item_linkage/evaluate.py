import os

import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import seaborn as sns
import typer
from loguru import logger
from matplotlib.backends.backend_pdf import PdfPages

from constants import (
    EVALUATION_FEATURES,
    EXPERIMENT_NAME,
    MLFLOW_RUN_ID_FILENAME,
    RUN_NAME,
)
from utils.common import read_parquet_files_from_gcs_directory
from utils.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment

app = typer.Typer()


def plot_graphs(
    linkage_candidates,
    output_file,
    merged_df,
    coverage_percentage,
    match_frequency_df,
    percentage_duplicates,
):
    with PdfPages(output_file) as pdf:
        # Coverage Pie Chart
        plt.figure(figsize=(12, 12))
        plt.pie(
            [coverage_percentage, 100 - coverage_percentage],
            labels=["Matched Offers", "Unmatched Offers"],
            autopct="%1.1f%%",
            startangle=140,
            colors=["#66b3ff", "#ffcc99"],
        )
        plt.title("Overall Coverage of Offers")
        plt.axis("equal")
        pdf.savefig()
        plt.close()

        # Match Frequency Histogram
        plt.figure(figsize=(16, 9))
        sns.histplot(
            match_frequency_df["match_count"],
            bins=range(1, int(match_frequency_df["match_count"].max()) + 2),
            kde=False,
            color="skyblue",
        )
        plt.title("Match Frequency Distribution")
        plt.xlabel("Number of Matches per Offer")
        plt.ylabel("Number of Offers")
        plt.xticks(range(1, match_frequency_df["match_count"].max() + 1))
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # Percentage of Duplicate Matches
        plt.figure(figsize=(16, 9))
        plt.bar(
            ["Duplicates", "Unique Matches"],
            [percentage_duplicates, 100 - percentage_duplicates],
            color=["#ff9999", "#99ff99"],
        )
        plt.title("Percentage of Duplicate Matches")
        plt.ylabel("Percentage (%)")
        plt.ylim(0, 100)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        if "booking_count" in linkage_candidates.columns:
            logger.info("Plot Booking Count Coverage...")

            total_booking_count = merged_df["booking_count"].sum()
            matched_booking_count = merged_df[merged_df["is_matched"]][
                "booking_count"
            ].sum()

            # Calculate the coverage percentage
            bookin_coverage_percentage = (
                matched_booking_count / total_booking_count
            ) * 100

            plt.figure(figsize=(12, 12))
            plt.pie(
                [bookin_coverage_percentage, 100 - bookin_coverage_percentage],
                labels=["Matched offers", "Unmatched offers"],
                autopct="%1.1f%%",
                startangle=140,
                colors=["#66b3ff", "#ffcc99"],
            )
            plt.title("Overall Booking Coverage")
            plt.axis("equal")
            pdf.savefig()
            plt.close()


def plot_subcat_graphs(linkage_candidates, output_file, merged_df, subcat_metrics):
    with PdfPages(output_file) as pdf:
        # Coverage by Subcategory
        plt.figure(figsize=(24, 12))
        sns.barplot(
            data=subcat_metrics,
            x="offer_subcategory_id_candidate",
            y="coverage_pct",
            palette="Blues_d",
        )
        plt.title("Coverage by Subcategory")
        plt.xlabel("Offer Subcategory ID")
        plt.ylabel("Coverage (%)")
        plt.xticks(rotation=45)
        plt.xticks(fontsize=6)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # Percentage of Duplicate Matches by Subcategory
        plt.figure(figsize=(24, 12))
        sns.barplot(
            data=subcat_metrics,
            x="offer_subcategory_id_candidate",
            y="duplicates_pct",
            palette="Reds_d",
        )
        plt.title("Percentage of Duplicate Matches by Subcategory")
        plt.xlabel("Offer Subcategory ID")
        plt.ylabel("Percentage of Duplicates (%)")
        plt.xticks(rotation=45)
        plt.xticks(fontsize=6)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        if (
            "booking_count" in linkage_candidates.columns
            and "offer_subcategory_id_candidate" in linkage_candidates.columns
        ):
            logger.info("Plot Booking Count Coverage by Offer Subcategory ID...")

            # Calculate total booking counts for all offers and matched offers
            total_booking_counts = (
                merged_df.groupby("offer_subcategory_id_candidate")["booking_count"]
                .sum()
                .reset_index()
            )
            matched_booking_counts = (
                merged_df[merged_df["is_matched"]]
                .groupby("offer_subcategory_id_candidate")["booking_count"]
                .sum()
                .reset_index()
            )

            # Merge the total and matched booking counts
            booking_coverage = pd.merge(
                total_booking_counts,
                matched_booking_counts,
                on="offer_subcategory_id_candidate",
                suffixes=("_total", "_matched"),
            )

            # Calculate the coverage percentage
            booking_coverage["coverage_percentage"] = (
                booking_coverage["booking_count_matched"]
                / booking_coverage["booking_count_total"]
                * 100
            )

            # Plot the data
            plt.figure(figsize=(14, 8))
            sns.barplot(
                data=booking_coverage,
                x="offer_subcategory_id_candidate",
                y="coverage_percentage",
                palette="viridis",
            )
            plt.xlabel("Offer Subcategory ID")
            plt.ylabel("Coverage Percentage")
            plt.title("Booking Count Coverage by Offer Subcategory ID")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            pdf.savefig()
            plt.close()


def evaluate_matching(
    linkage_candidates: pd.DataFrame,
    linked_items: pd.DataFrame,
    output_file: str,
):
    """
    Evaluates the matching between offers and products by computing various metrics,
    including coverage, and generating plots to visualize the results.

    Parameters:
    - linkage_candidates (pd.DataFrame): DataFrame containing all candidate offers.
    - linked_items (pd.DataFrame): DataFrame containing the results of the matching.
    - output_file (str): Filename to save the plots.
    """
    CANDIDATE_ID_COL = "item_id_candidate"
    merged_df = pd.merge(
        linkage_candidates,
        linked_items,
        on=CANDIDATE_ID_COL,
        how="left",
        indicator=True,
    )

    # Identify matched and unmatched offers
    merged_df = merged_df.assign(is_matched=lambda df: df._merge == "both")
    # merged_df["is_matched"] = merged_df["_merge"] == "both"

    # Calculate total number of offers and matched offers
    total_offers = linkage_candidates[CANDIDATE_ID_COL].nunique()
    total_matched_offers = merged_df[merged_df["is_matched"]][
        CANDIDATE_ID_COL
    ].nunique()

    # Calculate coverage
    coverage_percentage = (
        (total_matched_offers / total_offers) * 100 if total_offers > 0 else 0
    )

    # Calculate match frequency
    match_frequency = merged_df[merged_df["is_matched"]][
        CANDIDATE_ID_COL
    ].value_counts()
    match_frequency_df = match_frequency.reset_index()
    match_frequency_df.columns = [CANDIDATE_ID_COL, "match_count"]
    match_frequency_df.fillna(0, inplace=True)
    duplicate_matches_df = match_frequency_df[match_frequency_df["match_count"] > 1]
    num_duplicate_matches = duplicate_matches_df.shape[0]
    percentage_duplicates = (
        (num_duplicate_matches / total_matched_offers) * 100
        if total_matched_offers > 0
        else 0
    )

    plot_graphs(
        linkage_candidates,
        output_file,
        merged_df,
        coverage_percentage,
        match_frequency_df,
        percentage_duplicates,
    )


def evaluate_matching_by_subcategory(
    linkage_candidates: pd.DataFrame,
    linked_items: pd.DataFrame,
    output_file: str,
):
    """
    Evaluates the matching process within each subcategory, including coverage.

    Parameters:
    - linkage_candidates (pd.DataFrame): DataFrame containing all candidate offers.
    - linked_items (pd.DataFrame): DataFrame containing the results of the matching.
    - output_file (str): Filename to save the plots.
    """
    required_columns = ["item_id_candidate", "offer_subcategory_id_candidate"]
    for col in required_columns:
        if col not in linkage_candidates.columns or col not in linked_items.columns:
            raise ValueError(f"Both DataFrames must contain '{col}' column.")

    # Merge DataFrames on 'item_id_candidate' and 'offer_subcategory_id_candidate'
    merged_df = pd.merge(
        linkage_candidates,
        linked_items,
        on=required_columns,
        how="left",
        indicator=True,
    )

    # Identify matched offers
    merged_df = merged_df.assign(is_matched=lambda df: df._merge == "both")
    # merged_df["is_matched"] = merged_df["_merge"] == "both"

    # Group by subcategory and calculate metrics
    subcat_metrics = (
        merged_df.groupby("offer_subcategory_id_candidate")
        .apply(
            lambda df: pd.Series(
                {
                    "Total Offers": df["item_id_candidate"].nunique(),
                    "Matched Offers": df[df["is_matched"]][
                        "item_id_candidate"
                    ].nunique(),
                    "Total Matches": df[df["is_matched"]].shape[0],
                    "Duplicate Matches": df[df["is_matched"]]["item_id_candidate"]
                    .value_counts()
                    .gt(1)
                    .sum(),
                }
            )
        )
        .reset_index()
    )

    # Calculate additional metrics
    subcat_metrics = subcat_metrics.assign(
        coverage_pct=lambda df: df["Matched Offers"] / df["Total Offers"] * 100,
        duplicates_pct=lambda df: df["Duplicate Matches"] / df["Matched Offers"] * 100,
    ).fillna(0)

    # Generate plots and save them to a single PDF file
    plot_subcat_graphs(linkage_candidates, output_file, merged_df, subcat_metrics)


def build_evaluation_paths(linkage_type: str) -> dict:
    """
    Build evaluation paths for storing evaluation plots.

    Args:
        linkage_type (str): Type of linkage (product/offer).

    Returns:
        dict: Paths for overall and subcategory evaluation plots.
    """
    BASE_DIR = "plots"
    evaluation_dir = os.path.join(BASE_DIR, linkage_type)
    os.makedirs(evaluation_dir, exist_ok=True)
    return {
        "evaluation_plots": os.path.join(evaluation_dir, "evaluation_plots.pdf"),
        "subcategory_evaluation_plots": os.path.join(
            evaluation_dir, "subcategory_evaluation_plots.pdf"
        ),
    }


@app.command()
def main(
    linkage_type: str = typer.Option(default=..., help="Type of linkage to evaluate"),
    input_candidates_path: str = typer.Option(
        default=..., help="Path to linkage candidates"
    ),
    linkage_path: str = typer.Option(default=..., help="Path to linkage output"),
) -> None:
    """
    Evaluation of linkage results.

    This function:
      1) Reads the candidates from the specified path.
      2) Reads the final linkage output from another path.
      3) Renames columns as needed for consistency.
      4) Delegates to an evaluation function for further analysis.

    Args:
        linkage_type (str): The type of linkage to evaluate.
        input_candidates_path (str): The GCS path where linkage candidates are found.
        linkage_path (str): The GCS path where the final linkage output is found.
    """
    # Increase plot size and resolution
    candidates_clean = read_parquet_files_from_gcs_directory(
        input_candidates_path, columns=EVALUATION_FEATURES
    )
    candidates_clean = candidates_clean.rename(
        columns={
            "item_id": "item_id_candidate",
            "offer_subcategory_id": "offer_subcategory_id_candidate",
        }
    )

    linkage = read_parquet_files_from_gcs_directory(linkage_path)
    logger.info(f"linkage: {linkage.columns}")
    logger.info(f"candidates_clean: {candidates_clean.columns}")
    paths = build_evaluation_paths(linkage_type)
    logger.info(f"paths: {paths}")
    evaluate_matching(
        candidates_clean,
        linkage,
        output_file=paths["evaluation_plots"],
    )
    evaluate_matching_by_subcategory(
        candidates_clean,
        linkage,
        output_file=paths["subcategory_evaluation_plots"],
    )

    connect_remote_mlflow()
    experiment = get_mlflow_experiment(EXPERIMENT_NAME)
    with open(f"{MLFLOW_RUN_ID_FILENAME}.txt", mode="r") as file:
        run_id = file.read()
    with mlflow.start_run(
        experiment_id=experiment.experiment_id, run_id=run_id, run_name=RUN_NAME
    ):
        mlflow.log_artifact(
            "plots",
            artifact_path="",
        )

    return


if __name__ == "__main__":
    app()
