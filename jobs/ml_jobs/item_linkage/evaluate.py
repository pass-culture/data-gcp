import os

import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import seaborn as sns
import typer
from loguru import logger
from matplotlib.backends.backend_pdf import PdfPages

from constants import ENV_SHORT_NAME
from utils.common import read_parquet_files_from_gcs_directory
from utils.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment

app = typer.Typer()


def evaluate_matching(linkage_candidates, linked_items, output_file="evaluation_plots"):
    """
    Evaluates the matching between offers and products by computing various metrics,
    including coverage, and generating plots to visualize the results.

    Parameters:
    - linkage_candidates (pd.DataFrame): DataFrame containing all candidate offers.
    - linked_items (pd.DataFrame): DataFrame containing the results of the matching.
    - output_file (str): Filename to save the plots.

    Returns:
    - metrics_df (pd.DataFrame): DataFrame containing computed metrics.
    - match_frequency_df (pd.DataFrame): DataFrame containing match frequency data.
    - booking_count_coverage_df (pd.DataFrame): DataFrame containing booking count coverage data.
    """
    candidate_id_col = "item_id_candidate"
    # Merge the DataFrames to determine which offers have been matched
    merged_df = pd.merge(
        linkage_candidates,
        linked_items,
        on=candidate_id_col,
        how="left",
        indicator=True,
    )

    # Identify matched and unmatched offers
    merged_df["is_matched"] = merged_df["_merge"] == "both"

    # Calculate total number of offers and matched offers
    total_offers = linkage_candidates[candidate_id_col].nunique()
    total_matched_offers = merged_df[merged_df["is_matched"]][
        candidate_id_col
    ].nunique()

    # Calculate coverage
    coverage_percentage = (
        (total_matched_offers / total_offers) * 100 if total_offers > 0 else 0
    )

    # Calculate match frequency
    match_frequency = merged_df[merged_df["is_matched"]][
        candidate_id_col
    ].value_counts()
    match_frequency_df = match_frequency.reset_index()
    match_frequency_df.columns = [candidate_id_col, "match_count"]

    # Identify duplicate matches
    duplicate_matches_df = match_frequency_df[match_frequency_df["match_count"] > 1]
    num_duplicate_matches = duplicate_matches_df.shape[0]
    percentage_duplicates = (
        (num_duplicate_matches / total_matched_offers) * 100
        if total_matched_offers > 0
        else 0
    )

    # Compile metrics into a DataFrame
    # metrics = {
    #     "Total Offers": [total_offers],
    #     "Total Matched Offers": [total_matched_offers],
    #     "Coverage (%)": [coverage_percentage],
    #     "Number of Duplicate Matches": [num_duplicate_matches],
    #     "Percentage of Duplicates (%)": [percentage_duplicates],
    # }
    # metrics_df = pd.DataFrame(metrics)

    # Calculate Booking Count Coverage
    if "booking_count" in linkage_candidates.columns:
        booking_coverage = (
            merged_df.groupby("booking_count")
            .agg(
                total_offers=("booking_count", "count"),
                matched_offers=("is_matched", "sum"),
            )
            .reset_index()
        )
        booking_coverage["coverage_percentage"] = (
            booking_coverage["matched_offers"] / booking_coverage["total_offers"] * 100
        )
        booking_count_coverage_df = booking_coverage.copy()
    else:
        booking_count_coverage_df = pd.DataFrame()
        print("Warning: 'booking_count' column not found in linkage_candidates.")

    # Generate plots and save them to a single PDF file
    with PdfPages(output_file) as pdf:
        # Coverage Pie Chart
        plt.figure(figsize=(6, 6))
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
        plt.figure(figsize=(10, 6))
        sns.histplot(
            match_frequency_df["match_count"],
            bins=range(1, match_frequency_df["match_count"].max() + 2),
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
        plt.figure(figsize=(6, 6))
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

        # Booking Count Coverage Plot (if applicable)
        if not booking_count_coverage_df.empty:
            plt.figure(figsize=(10, 6))
            sns.barplot(
                data=booking_count_coverage_df,
                x="booking_count",
                y="coverage_percentage",
                palette="viridis",
            )
            plt.title("Coverage by Booking Count")
            plt.xlabel("Booking Count")
            plt.ylabel("Coverage Percentage (%)")
            plt.ylim(0, 100)
            plt.tight_layout()
            pdf.savefig()
            plt.close()

    return


def evaluate_matching_by_subcategory(
    linkage_candidates, linked_items, output_file="subcategory_evaluation_plots"
):
    """
    Evaluates the matching process within each subcategory, including coverage.

    Parameters:
    - linkage_candidates (pd.DataFrame): DataFrame containing all candidate offers.
    - linked_items (pd.DataFrame): DataFrame containing the results of the matching.
    - output_file (str): Filename to save the plots.

    Returns:
    - subcat_metrics_df (pd.DataFrame): DataFrame containing metrics per subcategory.
    """
    required_columns = ["item_id_candidate", "offer_subcategory_id"]
    for col in required_columns:
        if col not in linkage_candidates.columns or col not in linked_items.columns:
            raise ValueError(f"Both DataFrames must contain '{col}' column.")

    # Merge DataFrames on 'item_id_candidate' and 'offer_subcategory_id'
    merged_df = pd.merge(
        linkage_candidates,
        linked_items,
        on=required_columns,
        how="left",
        indicator=True,
    )

    # Identify matched offers
    merged_df["is_matched"] = merged_df["_merge"] == "both"

    # Group by subcategory and calculate metrics
    subcat_metrics = (
        merged_df.groupby("offer_subcategory_id")
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
    subcat_metrics["Coverage (%)"] = (
        subcat_metrics["Matched Offers"] / subcat_metrics["Total Offers"] * 100
    )
    subcat_metrics["Percentage of Duplicates (%)"] = (
        subcat_metrics["Duplicate Matches"] / subcat_metrics["Matched Offers"] * 100
    ).fillna(0)

    # Generate plots and save them to a single PDF file
    with PdfPages(output_file) as pdf:
        # Coverage by Subcategory
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=subcat_metrics,
            x="offer_subcategory_id",
            y="Coverage (%)",
            palette="Blues_d",
        )
        plt.title("Coverage by Subcategory")
        plt.xlabel("Offer Subcategory ID")
        plt.ylabel("Coverage (%)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

        # Percentage of Duplicate Matches by Subcategory
        plt.figure(figsize=(12, 6))
        sns.barplot(
            data=subcat_metrics,
            x="offer_subcategory_id",
            y="Percentage of Duplicates (%)",
            palette="Reds_d",
        )
        plt.title("Percentage of Duplicate Matches by Subcategory")
        plt.xlabel("Offer Subcategory ID")
        plt.ylabel("Percentage of Duplicates (%)")
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf.savefig()
        plt.close()

    return


def build_evaluation_paths(linkage_type: str, base_dir: str = "plots") -> dict:
    """
    Build evaluation paths for storing evaluation plots.

    Args:
        linkage_type (str): Type of linkage (product/offer).
        base_dir (str): Base directory for evaluation plots.

    Returns:
        dict: Paths for overall and subcategory evaluation plots.
    """
    evaluation_dir = os.path.join(base_dir, linkage_type)
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
    # Load input candidates based on linkage type
    load_columns = [
        "item_id",
        "performer",
        "offer_name",
        "offer_description",
        "offer_subcategory_id",
    ]

    product_linkage_candidates_clean = read_parquet_files_from_gcs_directory(
        input_candidates_path, columns=load_columns
    )

    # Load linkage results
    product_final_linkage = pd.read_parquet(linkage_path)

    # Rename columns for consistency
    product_linkage_candidates_clean = product_linkage_candidates_clean.rename(
        columns={"item_id": "item_id_candidate"}
    )
    logger.info(
        f"product_linkage_candidates_clean: {product_linkage_candidates_clean.columns}"
    )
    # Prepare evaluation paths
    paths = build_evaluation_paths(linkage_type)
    logger.info(f"paths: {paths}")
    # Run evaluations
    evaluate_matching(
        product_linkage_candidates_clean,
        product_final_linkage,
        output_file=paths["evaluation_plots"],
    )
    evaluate_matching_by_subcategory(
        product_linkage_candidates_clean,
        product_final_linkage,
        output_file=paths["subcategory_evaluation_plots"],
    )

    # Log results to MLflow
    experiment_name = f"item_linkage_v2.0_{ENV_SHORT_NAME}"
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id):
        mlflow.log_artifact(
            "plots",
            artifact_path="plots",
        )
        # mlflow.log_artifact(
        #     paths["subcategory_evaluation_plots"],
        #     artifact_path=paths["subcategory_evaluation_plots"],
        # )

    return


if __name__ == "__main__":
    app()
