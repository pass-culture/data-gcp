import pandas as pd
import os
import json
import typer
from fraud.offer_validation_model.utils.tools import preprocess, extract_embedding
from utils.constants import (
    STORAGE_PATH,
    MODEL_DIR,
    CONFIGS_PATH,
)


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

    offer_to_validate_raw = pd.read_gbq(
        "SELECT * FROM `passculture-data-ehp.sandbox_dev.offers_validation_test_sample_1602_0103`"
    )
    offer_to_validate_raw = offer_to_validate_raw.rename(
        columns={"image_url": "offer_image"}
    )

    offer_to_validate_clean = preprocess(offer_to_validate_raw)
    ## Extract emb
    offer_to_validate_clean_w_emb = extract_embedding(
        offer_to_validate_clean,
        features["features_to_extract_embedding"],
    )
    offer_to_validate_clean_w_emb.to_parquet(
        f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet"
    )
