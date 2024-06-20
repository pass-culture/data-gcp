import json
from pathlib import Path

import numpy as np
import pandas as pd
import typer

from fraud.offer_compliance_model.utils.constants import CONFIGS_PATH
from utils.constants import MODEL_DIR, STORAGE_PATH
from utils.data_collect_queries import read_from_gcs


def convert_str_emb_to_float(emb_list, emb_size=124):
    float_emb = []
    for str_emb in emb_list:
        try:
            emb = json.loads(str_emb)
        except Exception:
            emb = [0] * emb_size
        float_emb.append(np.array(emb))
    return float_emb


def prepare_features(df: pd.DataFrame, features: dict) -> pd.DataFrame:
    def _is_ndarray(val):
        return isinstance(val, np.ndarray)

    for feature_types in features["preprocess_features_type"].keys():
        for col in features["preprocess_features_type"][feature_types]:
            if feature_types == "text_features":
                df[col] = df[col].fillna("").astype(str)
            if feature_types == "numerical_features":
                df[col] = df[col].fillna(0).astype(int)
            if feature_types == "embedding_features":
                if not df[col].apply(_is_ndarray).all():
                    df[col] = convert_str_emb_to_float(df[col].tolist()).astype(
                        "object"
                    )
    return df


def filter_df_for_training(df: pd.DataFrame, features: dict) -> pd.DataFrame:
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
    config_file_path = Path(f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json")
    features = json.loads(config_file_path.read_text(encoding="utf-8"))

    offer_compliance_raw = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=input_dataframe_file_name
    )

    (
        offer_compliance_raw.pipe(prepare_features, features=features)
        .pipe(filter_df_for_training, features=features)
        .to_parquet(f"{STORAGE_PATH}/{output_dataframe_file_name}/data.parquet")
    )


if __name__ == "__main__":
    typer.run(preprocess)
