import json
import os

import pandas as pd
import typer
from fraud.offer_validation_model.utils.constants import CONFIGS_PATH
from fraud.offer_validation_model.utils.tools import extract_embedding, prepare_features
from utils.constants import MODEL_DIR, STORAGE_PATH
from utils.data_collect_queries import read_from_gcs


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
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    offer_to_validate_raw = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_dataframe_file_name
    )
    offer_to_validate_raw = offer_to_validate_raw.rename(
        columns={"image_url": "offer_image"}
    )

    offer_to_validate_clean = prepare_features(offer_to_validate_raw)
    ## Extract emb
    offer_to_validate_clean_w_emb = extract_embedding(
        offer_to_validate_clean,
        features["features_to_extract_embedding"],
    )
    offer_to_validate_clean_w_emb.to_parquet(
        f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet"
    )


if __name__ == "__main__":
    typer.run(preprocess)
