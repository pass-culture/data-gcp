from enum import Enum
from typing import Any, Dict, List

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder


class ClassMapping(Enum):
    seen = 0
    consulted = 1
    booked = 2


EMBEDDING_DIM = 0  # HACK : To remove embedding dimension from the model, set to 0
NUMERIC_FEATURES = (
    [
        "user_bookings_count",
        "user_clicks_count",
        "user_favorites_count",
        "user_theoretical_remaining_credit",
        "user_deposit_initial_amount",
        "user_is_geolocated",
        "user_iris_x",
        "user_iris_y",
        "offer_user_distance",
        "offer_booking_number_last_7_days",
        "offer_booking_number_last_14_days",
        "offer_booking_number_last_28_days",
        "offer_semantic_emb_mean",
        "offer_item_score",
        "offer_is_geolocated",
        "offer_stock_price",
        "offer_creation_days",
        "offer_stock_beginning_days",
        "day_of_the_week",
        "hour_of_the_day",
        "offer_centroid_x",
        "offer_centroid_y",
    ]
    + [f"user_emb_{i}" for i in range(EMBEDDING_DIM)]
    + [f"item_emb_{i}" for i in range(EMBEDDING_DIM)]
)

CATEGORICAL_FEATURES = ["context", "offer_subcategory_id"]

DEFAULT_CATEGORICAL = "UNKNOWN"
DEFAULT_NUMERICAL = -1


class PredictPipeline:
    def __init__(self) -> None:
        self.numeric_features = NUMERIC_FEATURES
        self.categorical_features = CATEGORICAL_FEATURES
        self.model_classifier = lgb.Booster(
            model_file="./metadata/model_classifier.txt"
        )
        self.preprocessor_classifier = joblib.load(
            "./metadata/preproc_classifier.joblib"
        )

    def predict(self, input_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Predict class and scores based on input data using a pre-trained model.

        Parameters:
        ----------
        input_data : List[Dict[str, Any]]
            A list of dictionaries where each dictionary contains features used for prediction.
            The features include both numerical and categorical ones. Missing features will be replaced
            with default values specified in the system.

        Returns:
        -------
        List[Dict[str, Any]]
            A list of dictionaries where each dictionary contains the offer_id and the score.
        """

        processed_data: List[Dict[str, Any]] = []

        for item in input_data:
            processed_item = {}

            for feature in self.numeric_features:
                processed_item[feature] = item.get(feature, DEFAULT_NUMERICAL)

            for feature in self.categorical_features:
                processed_item[feature] = item.get(feature, DEFAULT_CATEGORICAL)

            processed_data.append(processed_item)

        processed_data_classifier = self.preprocessor_classifier.transform(
            pd.DataFrame(processed_data)
        )
        predictions_classifier = self.model_classifier.predict(
            processed_data_classifier
        )

        results: List[Dict[str, Any]] = []
        for idx, prediction in enumerate(predictions_classifier):
            score = float(
                prediction[ClassMapping.consulted.value]
                + prediction[ClassMapping.booked.value]
            )
            offer_id = input_data[idx].get("offer_id")
            result = {
                "offer_id": offer_id,
                "score": score,
            }
            results.append(result)

        return results


class TrainPipeline:
    def __init__(self, target: str, params: dict) -> None:
        self.numeric_features = NUMERIC_FEATURES
        self.categorical_features = CATEGORICAL_FEATURES
        self.preprocessor: ColumnTransformer = None
        self.train_size = 0.9
        self.target = target
        self.params = params

    def set_pipeline(self):
        numeric_transformer = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="constant", fill_value=DEFAULT_NUMERICAL),
                )
            ]
        )

        categorical_transformer = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="constant", fill_value=DEFAULT_CATEGORICAL),
                ),
                (
                    "encoder",
                    OrdinalEncoder(
                        handle_unknown="use_encoded_value", unknown_value=-1
                    ),
                ),
            ]
        )

        self.preprocessor = ColumnTransformer(
            transformers=[
                ("num", numeric_transformer, self.numeric_features),
                ("cat", categorical_transformer, self.categorical_features),
            ]
        )

    def fit_transform(self, df):
        df[self.categorical_features] = (
            df[self.categorical_features].astype(str).fillna(DEFAULT_CATEGORICAL)
        )
        df[self.numeric_features] = (
            df[self.numeric_features].astype(float).fillna(DEFAULT_NUMERICAL)
        )
        return self.preprocessor.fit_transform(df)

    def transform(self, input_data):
        df = pd.DataFrame(input_data)
        processed_data = self.preprocessor.transform(df)
        processed_df = pd.DataFrame(processed_data)
        return processed_df.to_numpy()

    def save(self, model_name: str):
        joblib.dump(self.preprocessor, f"./metadata/preproc_{model_name}.joblib")
        self.model.save_model(f"./metadata/model_{model_name}.txt")

    def train(self, df: pd.DataFrame, class_weight: dict, seed: int):
        # Split data based on user_x_date_id
        unique_user_x_date_ids = df.user_x_date_id.unique()
        train_session_ids, test_session_ids = train_test_split(
            unique_user_x_date_ids, test_size=self.train_size, random_state=seed
        )
        train_data = df[df["user_x_date_id"].isin(train_session_ids)]
        test_data = df[df["user_x_date_id"].isin(test_session_ids)]

        # Preprocess for lgbm
        X_train = self.fit_transform(train_data)
        X_test = self.transform(test_data)
        y_train, y_test = (
            train_data[self.target].to_numpy(),
            test_data[self.target].to_numpy(),
        )

        train_data = lgb.Dataset(
            X_train,
            y_train,
            feature_name=self.numeric_features + self.categorical_features,
            weight=(
                np.array([class_weight[label] for label in y_train])
                if class_weight
                else None
            ),
        )
        test_data = lgb.Dataset(
            X_test,
            y_test,
            feature_name=self.numeric_features + self.categorical_features,
            weight=(
                np.array([class_weight[label] for label in y_test])
                if class_weight
                else None
            ),
        )

        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=50_000,
            valid_sets=[train_data, test_data],
            callbacks=[lgb.early_stopping(stopping_rounds=200)],
        )

    def predict_classifier(self, df: pd.DataFrame) -> pd.DataFrame:
        processed_data = self.preprocessor.transform(df)
        probabilities = self.model.predict(processed_data)
        predicted_class = probabilities.argmax(axis=1)

        return df.assign(
            **{
                "predicted_class": predicted_class,
                "score": probabilities[:, ClassMapping.consulted.value]
                + probabilities[:, ClassMapping.booked.value],
                **{
                    f"prob_class_{class_mapping.name}": probabilities[
                        :, class_mapping.value
                    ]
                    for class_mapping in ClassMapping
                },
            }
        )

    def predict_regressor(self, df: pd.DataFrame) -> pd.DataFrame:
        processed_data = self.preprocessor.transform(df)

        return df.assign(regression_score=self.model.predict(processed_data))
