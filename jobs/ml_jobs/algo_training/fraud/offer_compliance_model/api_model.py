import io
from heapq import nlargest, nsmallest

import mlflow
import numpy as np
import pandas as pd
import requests
import shap
from catboost import CatBoostClassifier, Pool
from PIL import Image
from sentence_transformers import SentenceTransformer

from .preprocess import prepare_features


class ApiModel(mlflow.pyfunc.PythonModel):
    TEXT_ENCODER_MODEL = SentenceTransformer(
        "sentence-transformers/clip-ViT-B-32-multilingual-v1"
    )
    IMAGE_ENCODER_MODEL = SentenceTransformer("clip-ViT-B-32")
    SEMENTIC_CONTENT_COLUMNS = [
        "offer_name",
        "offer_description",
        "offer_type_label",
        "offer_sub_type_label",
        "author",
        "performer",
    ]

    def __init__(self, model_classifier: CatBoostClassifier, features: dict):
        self.model_classifier = model_classifier
        self.features = features

    def predict(self, model_input: pd.DataFrame):
        """
        Predicts the class labels for the given data using the trained classifier model.

        Args:
            api_config (dict): Configuration parameters for the API.
            model_config (dict): Configuration parameters for the model.
            data (list): Input data to be predicted.

        Returns:
            tuple: A tuple containing the predicted class labels and the main contribution.
                offer validition probability
                offer rejection probability (=1-proba_val)
                main features contributing to increase validation probability
                main features contributing to reduce validation probability
        """
        input_df = pd.DataFrame([model_input])

        # Preprocess the data and the embedder
        pool, data_w_emb = self.preprocess(input_df)
        print(pool)
        print(data_w_emb)

        # Run the prediction
        return self.get_prediction_and_main_contribution(data_w_emb, pool)

    def preprocess(self, input_df: pd.DataFrame):
        """
        Preprocessing steps:
            - prepare features
            - convert json data to catboost pool
        """
        input_with_embeddings_df = (
            input_df.pipe(self._extract_embedding)
            .pipe(prepare_features, features=self.features)
            .loc[:, self._get_catboost_features()]
        )

        pool = self._convert_data_to_catboost_pool(input_with_embeddings_df)
        return pool, input_with_embeddings_df

    def _get_catboost_features(self):
        return self.model_classifier.feature_names_

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
                self.SEMENTIC_CONTENT_COLUMNS
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
        return self.TEXT_ENCODER_MODEL.encode(
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
            img_emb = self.IMAGE_ENCODER_MODEL.encode(
                Image.open(io.BytesIO(requests.get(url).content))
            )
            offer_img_embs = img_emb
        except Exception:
            offer_img_embs = np.array([0] * 512)
        return offer_img_embs

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
        catboost_features_types = self.features["catboost_features_types"]

        print(self._get_catboost_features())
        return Pool(
            data=input_with_embeddings_df,
            feature_names=input_with_embeddings_df.columns.tolist(),
            cat_features=catboost_features_types["cat_features"],
            text_features=catboost_features_types["text_features"],
            embedding_features=catboost_features_types["embedding_features"],
        )

    def get_prediction_and_main_contribution(self, data_w_emb, pool):
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

        proba_predicted = self.model_classifier.predict(
            pool,
            prediction_type="Probability",
            ntree_start=0,
            ntree_end=0,
            thread_count=1,
            verbose=None,
        )[0]
        proba_rej = list(proba_predicted)[0] * 100
        proba_val = list(proba_predicted)[1] * 100
        top_val, top_rej = self._get_prediction_main_contribution(data_w_emb, pool)
        return proba_val, proba_rej, top_val, top_rej

    def _get_prediction_main_contribution(self, data, pool):
        explainer = shap.Explainer(self.model_classifier, link=shap.links.logit)
        shap_values = explainer.shap_values(pool)
        top_val, top_rej = self.__get_contribution_from_shap_values(shap_values, data)
        return top_val, top_rej

    @staticmethod
    def __get_contribution_from_shap_values(shap_values, data):
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

        topk_rejection_factor = [
            data_keys[individual_shap_values.index(min_val)] for min_val in ksmallest
        ]
        return topk_validation_factor, topk_rejection_factor
