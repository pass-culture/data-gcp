import json
import os

import pandas as pd
import typer
from fraud.offer_compliance_model.utils.constants import CONFIGS_PATH
from fraud.offer_compliance_model.utils.tools import extract_embedding, prepare_features
from utils.constants import MODEL_DIR, STORAGE_PATH
from utils.data_collect_queries import read_from_gcs


IMAGE_MODEL = SentenceTransformer("clip-ViT-B-32")
TEXT_MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


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

    offer_compliance_raw = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_dataframe_file_name
    )
    offer_compliance_raw = offer_compliance_raw.rename(
        columns={"image_url": "offer_image"}
    )

    offer_compliance_clean = prepare_features(offer_compliance_raw)
    ## Extract emb
    offer_compliance_clean_w_emb = extract_embedding(
        offer_compliance_clean,
        features["features_to_extract_embedding"],
        IMAGE_MODEL,
        TEXT_MODEL,
    )
    offer_compliance_clean_w_emb.to_parquet(
        f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet"
    )


if __name__ == "__main__":
    typer.run(preprocess)
