import json
import os
from sentence_transformers import SentenceTransformer

import pandas as pd
import numpy as np
import typer
from fraud.offer_compliance_model.utils.constants import CONFIGS_PATH
from fraud.offer_compliance_model.utils.tools import extract_embedding, prepare_features
from utils.constants import MODEL_DIR, STORAGE_PATH
from utils.data_collect_queries import read_from_gcs


IMAGE_MODEL = SentenceTransformer("clip-ViT-B-32")
TEXT_MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def convert_str_emb_to_float(emb_list, emb_size=124):
    float_emb = []
    for str_emb in emb_list:
        try:
            emb = json.loads(str_emb)
        except:
            emb = [0] * emb_size
        float_emb.append(np.array(emb))
    return float_emb


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

    offer_compliance_w_emb = offer_compliance_clean.loc[
        offer_compliance_clean["image_embedding"] != ""
    ]
    for feature_name in features["catboost_features_types"]["embedding_features"]:
        offer_compliance_w_emb[f"""{feature_name}"""] = convert_str_emb_to_float(
            offer_compliance_w_emb[f"""{feature_name}"""].tolist()
        )
        offer_compliance_w_emb[f"""{feature_name}"""] = offer_compliance_w_emb[
            f"""{feature_name}"""
        ].astype("object")

    offer_compliance_w_emb = offer_compliance_w_emb.drop(
        [
            "offer_name",
            "offer_description",
            "offer_image",
        ],
        axis=1,
        errors="ignore",
    )
    offer_compliance_w_emb.to_parquet(
        f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet"
    )


if __name__ == "__main__":
    typer.run(preprocess)
