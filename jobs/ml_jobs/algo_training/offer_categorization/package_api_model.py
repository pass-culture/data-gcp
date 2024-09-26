import json
from dataclasses import dataclass

import mlflow
import numpy as np
import pandas as pd
import typer
from catboost import CatBoostClassifier, Pool
from sentence_transformers import SentenceTransformer

from offer_categorization.config import features
from utils.constants import ENV_SHORT_NAME
from utils.mlflow_tools import connect_remote_mlflow


@dataclass
class PredictionInput:
    offer_name: str
    offer_description: str
    offerer_name: str
    venue_type_label: str


@dataclass
class PredictionOutput:
    subcategory: list
    probability: list


@dataclass
class PreprocessingOutput:
    data_with_embeddings_df: pd.DataFrame
    pool: Pool


class ApiModel(mlflow.pyfunc.PythonModel):
    TEXT_ENCODER_MODEL = SentenceTransformer(
        "sentence-transformers/all-MiniLM-L6-v2", device="cpu"
    )

    def __init__(self, classification_model: CatBoostClassifier, features: dict):
        self.classification_model = classification_model
        self.features = features
        self.preprocessing_pipeline = PreprocessingPipeline(
            text_encoder=self.TEXT_ENCODER_MODEL,
            features_description=self.features,
            catboost_features=self._get_catboost_features(),
        )
        self.classification_pipeline = ClassificationPipeline(
            classification_model=self.classification_model
        )

    def _get_catboost_features(self):
        return self.classification_model.feature_names_

    def predict(self, context, model_input: PredictionInput) -> PredictionOutput:
        input_df = pd.DataFrame([model_input])

        # Preprocess the data and the embedder
        preprocessing_output = self.preprocessing_pipeline(input_df)

        # Run the prediction
        return self.classification_pipeline(preprocessing_output)


class ClassificationPipeline:
    def __init__(self, classification_model: CatBoostClassifier):
        self.classification_model = classification_model

    def __call__(self, preprocessing_output: PreprocessingOutput) -> PredictionOutput:
        return PredictionOutput(
            subcategory=self.classification_model.classes_.tolist(),
            probability=self.classification_model.predict_proba(
                preprocessing_output.pool
            ).tolist()[0],
        )


class PreprocessingPipeline:
    def __init__(
        self,
        text_encoder: SentenceTransformer,
        features_description: dict,
        catboost_features: list,
    ):
        self.text_encoder = text_encoder
        self.features_description = features_description
        self.catboost_features = catboost_features

    def __call__(self, input_df: pd.DataFrame) -> PreprocessingOutput:
        input_with_embeddings_df = (
            input_df.pipe(self._extract_embedding)
            .pipe(self.prepare_features, features_description=self.features_description)
            .loc[:, self.catboost_features]
        )
        pool = self._convert_data_to_catboost_pool(input_with_embeddings_df)
        return PreprocessingOutput(
            data_with_embeddings_df=input_with_embeddings_df, pool=pool
        )

    def _extract_embedding(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract embedding with pretrained models
        - text  :
            - Input: list of string
        Params template:
        [
            {"name": "offer_name", "type": "text"},
            {"name": "offer_description", "type": "text"},
        ]
        """
        return input_df.assign(
            name_embedding=lambda df: df["offer_name"].apply(
                lambda row: self.text_encoder.encode(row).tolist(),
            ),
            description_embedding=lambda df: df["offer_description"].apply(
                lambda row: self.text_encoder.encode(row).tolist(),
            ),
        )

    @classmethod
    def prepare_features(
        cls, df: pd.DataFrame, features_description: dict
    ) -> pd.DataFrame:
        def _is_ndarray(val):
            return isinstance(val, np.ndarray)

        for feature_types in features_description["preprocess_features_type"].keys():
            for col in features_description["preprocess_features_type"][feature_types]:
                if feature_types == "text_features":
                    df[col] = df[col].fillna("").astype(str)
                if feature_types == "embedding_features":
                    if not df[col].apply(_is_ndarray).all():
                        df[col] = cls._convert_str_emb_to_float(df[col].tolist())

        return df

    @staticmethod
    def _convert_str_emb_to_float(emb_list, emb_size=124):
        float_emb = []
        for str_emb in emb_list:
            try:
                emb = json.loads(str_emb)
            except Exception:
                emb = [0] * emb_size
            float_emb.append(np.array(emb))
        return float_emb

    def _convert_data_to_catboost_pool(
        self, input_with_embeddings_df: pd.DataFrame
    ) -> Pool:
        """
        Convert json data to catboost pool:
            - inputs:
                - Features names: List of features name (same order as list of features)
                - cat_features: list of categorical features names
                - text_features: list of text features names
                - embedding_features: list of embedding features names
            - output:
                - catboost pool
        """
        catboost_features_types = self.features_description["catboost_features_types"]

        return Pool(
            data=input_with_embeddings_df,
            feature_names=input_with_embeddings_df.columns.tolist(),
            cat_features=catboost_features_types["cat_features"],
            text_features=catboost_features_types["text_features"],
            embedding_features=catboost_features_types["embedding_features"],
        )


def package_api_model(
    model_name: str = typer.Option(
        "compliance_default", help="Model name for the training"
    ),
):
    # Connect to MLflow
    connect_remote_mlflow()
    # Build the API model
    catboost_model = mlflow.catboost.load_model(
        model_uri=f"models:/{model_name}_{ENV_SHORT_NAME}/latest"
    )

    api_model = ApiModel(
        classification_model=catboost_model, features=features["default"]
    )

    api_model_name = f"api_{model_name}_{ENV_SHORT_NAME}"
    mlflow.pyfunc.log_model(
        python_model=api_model,
        artifact_path=f"registry_{ENV_SHORT_NAME}",
        registered_model_name=api_model_name,
    )

    # Add metadata
    client = mlflow.MlflowClient()
    training_run_id = client.get_latest_versions(f"{model_name}_{ENV_SHORT_NAME}")[
        0
    ].run_id
    api_model_version = client.get_latest_versions(api_model_name)[0].version
    client.set_model_version_tag(
        name=api_model_name,
        version=api_model_version,
        key="training_run_id",
        value=training_run_id,
    )
    client.set_registered_model_alias(api_model_name, "production", api_model_version)


if __name__ == "__main__":
    typer.run(package_api_model)
