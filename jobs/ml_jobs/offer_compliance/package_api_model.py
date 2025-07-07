import io
import json
from dataclasses import dataclass
from heapq import nlargest, nsmallest

import mlflow
import numpy as np
import pandas as pd
import requests
import shap
import typer
from catboost import CatBoostClassifier, Pool
from PIL import Image
from sentence_transformers import SentenceTransformer

from constants import (
    CONFIGS_PATH,
    ENV_SHORT_NAME,
)
from utils.mlflow_tools import connect_remote_mlflow


@dataclass
class PredictionInput:
    offer_name: str
    offer_description: str
    offer_subcategory_id: str
    rayon: str
    macro_rayon: str
    stock_price: int
    image_url: str
    offer_type_label: str
    offer_sub_type_label: str


@dataclass
class PredictionOutput:
    probability_validated: float
    validation_main_features: list
    probability_rejected: float
    rejection_main_features: list


@dataclass
class PreprocessingOutput:
    pool: Pool
    data_with_embeddings_df: pd.DataFrame


class ApiModel(mlflow.pyfunc.PythonModel):
    TEXT_ENCODER_MODEL = SentenceTransformer(
        model_name_or_path="sentence-transformers/clip-ViT-B-32-multilingual-v1",
        device="cpu",
    )
    IMAGE_ENCODER_MODEL = SentenceTransformer(
        model_name_or_path="clip-ViT-B-32", device="cpu"
    )
    SEMENTIC_CONTENT_COLUMNS = [  # noqa: RUF012
        "offer_name",
        "offer_description",
        "offer_type_label",
        "offer_sub_type_label",
        "author",
        "performer",
    ]

    def __init__(self, classification_model: CatBoostClassifier, features: dict):
        self.classification_model = classification_model
        self.features = features
        self.preprocessing_pipeline = PreprocessingPipeline(
            text_encoder=self.TEXT_ENCODER_MODEL,
            image_encoder=self.IMAGE_ENCODER_MODEL,
            semantic_content_columns=self.SEMENTIC_CONTENT_COLUMNS,
            features_description=self.features,
            catboost_features=self._get_catboost_features(),
        )
        self.classification_pipeline = ClassificationPipeline(
            classification_model=self.classification_model
        )

    def _get_catboost_features(self):
        return self.classification_model.feature_names_

    def predict(self, context, model_input: PredictionInput) -> PredictionOutput:
        """
        Predicts the class labels for the given data using the trained classifier model.

        Args:
            api_config (dict): Configuration parameters for the API.
            model_config (dict): Configuration parameters for the model.
            data (list): Input data to be predicted.

        Returns:
            tuple: the predicted class labels and the main contribution.
                offer validition probability
                offer rejection probability (=1-proba_val)
                main features contributing to increase validation probability
                main features contributing to reduce validation probability
        """
        input_df = pd.DataFrame([model_input])

        # Preprocess the data and the embedder
        preprocessing_output = self.preprocessing_pipeline(input_df)

        # Run the prediction
        return self.classification_pipeline(preprocessing_output)


class ClassificationPipeline:
    def __init__(
        self,
        classification_model: CatBoostClassifier,
    ):
        self.classification_model = classification_model

    def __call__(self, preprocessing_output: PreprocessingOutput) -> PredictionOutput:
        """
        Prediction:
            Predict validation/rejection probability for a given input as catboost pool
            inputs:
                - pool: Catboost pool with offer features
                - model: Catboost custom trained model
            outputs:
                proba_val: offer validition probability
                proba_rej: offer rejection probability (=1-proba_val)
        Main contribution:
            Extract prediction main contribution features from shap values
            inputs:
                - model: Catboost custom trained model
                - data: json with offer features
                - pool: Catboost with offer features
            outputs:
                top_val: main features contributing to increase validation probability
                top_reg: main features contributing to reduce validation probability
        """

        proba_predicted = list(
            self.classification_model.predict(
                preprocessing_output.pool,
                prediction_type="Probability",
                ntree_start=0,
                ntree_end=0,
                thread_count=1,
                verbose=None,
            )[0]
        )
        proba_rejection = proba_predicted[0] * 100
        proba_validation = proba_predicted[1] * 100
        top_validation, top_rejection = self._get_prediction_main_contribution(
            preprocessing_output.data_with_embeddings_df, preprocessing_output.pool
        )
        return PredictionOutput(
            probability_validated=proba_validation,
            validation_main_features=top_validation,
            probability_rejected=proba_rejection,
            rejection_main_features=top_rejection,
        )

    def _get_prediction_main_contribution(
        self, data: pd.DataFrame, pool: Pool
    ) -> tuple[list, list]:
        explainer = shap.Explainer(self.classification_model, link=shap.links.logit)
        shap_values = explainer.shap_values(pool)
        top_val, top_rej = self.__get_contribution_from_shap_values(shap_values, data)
        return top_val, top_rej

    @staticmethod
    def __get_contribution_from_shap_values(shap_values, data: pd.DataFrame):
        topk_validation_factor = []
        topk_rejection_factor = []
        data_keys = list(data.keys())
        # for i in range(len(data)):
        individual_shap_values = list(shap_values[0, :])
        klargest = nlargest(3, individual_shap_values)
        ksmallest = nsmallest(3, individual_shap_values)
        topk_validation_factor = [
            data_keys[individual_shap_values.index(max_val)] for max_val in klargest
        ]

        #
        topk_rejection_factor = [
            data_keys[individual_shap_values.index(min_val)] for min_val in ksmallest
        ]
        return topk_validation_factor, topk_rejection_factor


class PreprocessingPipeline:
    def __init__(
        self,
        text_encoder: SentenceTransformer,
        image_encoder: SentenceTransformer,
        semantic_content_columns: list,
        features_description: dict,
        catboost_features: list,
    ):
        self.text_encoder = text_encoder
        self.image_encoder = image_encoder
        self.sementic_content_columns = semantic_content_columns
        self.features_description = features_description
        self.catboost_features = catboost_features

    def __call__(self, input_df: pd.DataFrame) -> PreprocessingOutput:
        """
        Preprocessing steps:
            - prepare features
            - convert json data to catboost pool
        """
        input_with_embeddings_df = (
            input_df.pipe(self._extract_embedding)
            .pipe(self.prepare_features, features_description=self.features_description)
            .loc[:, self.catboost_features]
        )

        pool = self._convert_data_to_catboost_pool(input_with_embeddings_df)
        return PreprocessingOutput(
            pool=pool, data_with_embeddings_df=input_with_embeddings_df
        )

    def _extract_embedding(self, input_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract embedding with pretrained models
        Two types available:
        - image :
            - Input: list of urls
        - text  :
            - Input: list of string
        Params template:
        [
            {"name": "offer_name", "type": "text"},
            {"name": "offer_description", "type": "text"},
            {"name": "image_url", "type": "image"},
        ]
        """
        return input_df.assign(
            image_embedding=lambda df: df["image_url"].map(
                lambda image_url: self._encode_img_from_url(url=image_url)
            ),
            semantic_content_embedding=lambda df: df[
                self.sementic_content_columns
            ].apply(
                lambda row: self._encode_sementic_content(
                    semenctic_content_series=row,
                ),
                axis=1,
            ),
        )

    def _encode_sementic_content(self, semenctic_content_series: pd.Series):
        """
        Encode text with pre-trained model
        """
        return self.text_encoder.encode(
            " ".join(semenctic_content_series.astype(str).values)
        )

    def _encode_img_from_url(self, url: str) -> np.ndarray:
        """
        Encode image with pre-trained model from url

        inputs:
            - model : HugginFaces pre-trained model using Sentence-Transformers
            - url : string of image url
        """
        offer_img_embs = []
        try:
            img_emb = self.image_encoder.encode(
                Image.open(io.BytesIO(requests.get(url).content))
            )
            offer_img_embs = img_emb
        except Exception:
            offer_img_embs = np.array([0] * 512)
        return offer_img_embs

    @classmethod
    def prepare_features(
        cls, df: pd.DataFrame, features_description: dict
    ) -> pd.DataFrame:
        def _is_str_emb(val):
            return isinstance(val, str)

        for feature_types in features_description["preprocess_features_type"]:
            for col in features_description["preprocess_features_type"][feature_types]:
                if feature_types == "text_features":
                    df[col] = df[col].fillna("").astype(str)
                if feature_types == "numerical_features":
                    df[col] = df[col].fillna(0).astype(int)
                if (
                    feature_types == "embedding_features"
                    and df[col].apply(_is_str_emb).all()
                ):
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
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
    ),
):
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)

    # Connect to MLflow
    connect_remote_mlflow()

    # Build the API model
    catboost_model = mlflow.catboost.load_model(
        model_uri=f"models:/{model_name}_{ENV_SHORT_NAME}/latest"
    )
    api_model = ApiModel(classification_model=catboost_model, features=features)

    # Register the API model
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
