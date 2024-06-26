import json

import numpy as np
import typer
from fraud.offer_compliance_model.utils.constants import CONFIGS_PATH
from sentence_transformers import SentenceTransformer
from utils.constants import MODEL_DIR, STORAGE_PATH
from utils.data_collect_queries import read_from_gcs

IMAGE_MODEL = SentenceTransformer("clip-ViT-B-32")
TEXT_MODEL = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def convert_str_emb_to_float(emb_list, emb_size=124):
    float_emb = []
    for str_emb in emb_list:
        try:
            emb = json.loads(str_emb)
        except Exception:
            emb = [0] * emb_size
        float_emb.append(np.array(emb))
    return float_emb


def prepare_features(df, features):
    for feature_types in features["preprocess_features_type"].keys():
        for col in features["preprocess_features_type"][feature_types]:
            if feature_types == "text_features":
                df[col] = df[col].fillna("")
                df[col] = df[col].astype(str)
            if feature_types == "numerical_features":
                df[col] = df[col].fillna(0)
                df[col] = df[col].astype(int)
            if feature_types == "embedding_features":
                df[col] = convert_str_emb_to_float(df[col].tolist())
                df[col] = df[col].astype("object")

    scoring_features = sum(features["catboost_features_types"].values(), [])
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
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    offer_compliance_raw = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_dataframe_file_name
    )

    offer_compliance_clean = prepare_features(offer_compliance_raw, features)
    offer_compliance_clean.to_parquet(
        f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet"
    )


if __name__ == "__main__":
    typer.run(preprocess)
