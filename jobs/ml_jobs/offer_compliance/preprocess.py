import functools
import json
import operator
from pathlib import Path

import numpy as np
import pandas as pd
import typer

from constants import CONFIGS_PATH, STORAGE_PATH
from package_api_model import PreprocessingPipeline
from utils.data_collect_queries import read_from_gcs


def filter_df_for_training(df: pd.DataFrame, features: dict) -> pd.DataFrame:
    scoring_features = functools.reduce(
        operator.iadd, features["catboost_features_types"].values(), []
    )
    # Set target
    df["target"] = np.where(df["offer_validation"] == "APPROVED", 1, 0)
    scoring_features.append("target")
    # Only keep features used for scoring
    df = df[scoring_features]
    return df


def preprocess(
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
    input_dataframe_file_name: str = typer.Option(
        ...,
        help="Name of the dataframe we want to clean",
    ),
    output_dataframe_file_name: str = typer.Option(
        ...,
        help="Name of the cleaned dataframe",
    ),
):
    config_file_path = Path(f"{CONFIGS_PATH}/{config_file_name}.json")
    features = json.loads(config_file_path.read_text(encoding="utf-8"))

    offer_compliance_raw = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_dataframe_file_name
    )

    (
        offer_compliance_raw.pipe(
            PreprocessingPipeline.prepare_features, features_description=features
        )
        .pipe(filter_df_for_training, features=features)
        .to_parquet(f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet")
    )


if __name__ == "__main__":
    typer.run(preprocess)
