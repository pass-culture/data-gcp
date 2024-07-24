import json
import pandas as pd
import numpy as np
import typer
from offer_categorization.package_api_model import PreprocessingPipeline
from offer_categorization.config import features
from utils.gcs_utils import upload_parquet
from loguru import logger

app = typer.Typer()


def assign_label(df: pd.DataFrame) -> pd.DataFrame:
    ll = sorted(df.offer_subcategory_id.unique())
    label_mapping = pd.DataFrame(
        {"offer_subcategory_id": ll, "label": list(range(len(ll)))}
    )
    data_with_label = pd.merge(df, label_mapping, on=["offer_subcategory_id"])

    return data_with_label


def save_label_mapping(df: pd.DataFrame, output_file_path: str):
    df.to_parquet(output_file_path)


@app.command()
def preprocess(
    input_table_path: str = typer.Option("", help="Path to the input table."),
    output_file_dir: str = typer.Option("", help="Directory to save the output files."),
    config_name: str = typer.Option("default", help="Configuration name."),
) -> None:
    features_description = features[config_name]
    logger.info("Preprocess data...")
    data_clean = (
        pd.read_parquet(input_table_path)
        .pipe(
            PreprocessingPipeline.prepare_features,
            features_description=features_description,
        )
        .pipe(assign_label)
    )
    data_clean = data_clean.drop(["offer_id", "item_id"], axis=1)
    save_label_mapping(
        data_clean[["offer_subcategory_id", "label"]]
        .drop_duplicates()
        .reset_index(drop=True),
        f"{output_file_dir}/label_mapping.parquet",
    )
    logger.info("Save preprocessed data")
    upload_parquet(
        dataframe=data_clean,
        gcs_path=f"{output_file_dir}/data_clean.parquet",
    )


if __name__ == "__main__":
    app()
