from typing import Optional

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from app.constants import ClassMapping

NUMERIC_FEATURES = [
    "user_bookings_count",
    "user_clicks_count",
    "user_favorites_count",
    "user_deposit_remaining_credit",
    "user_is_geolocated",
    "user_iris_x",
    "user_iris_y",
    "offer_user_distance",
    "offer_booking_number_last_7_days",
    "offer_booking_number_last_14_days",
    "offer_booking_number_last_28_days",
    "offer_semantic_emb_mean",
    "offer_item_score",
    "offer_item_rank",
    "offer_is_geolocated",
    "offer_stock_price",
    "offer_creation_days",
    "offer_stock_beginning_days",
    "day_of_the_week",
    "hour_of_the_day",
]

CATEGORICAL_FEATURES = [
    "context",
    "offer_subcategory_id",
]

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

    def predict(self, input_data: list[dict]):
        errors = []
        df = pd.DataFrame(input_data)
        _cols = list(df.columns)

        for x in self.numeric_features:
            if x not in _cols:
                errors.append(x)
                df[x] = DEFAULT_NUMERICAL
        for x in self.categorical_features:
            if x not in _cols:
                errors.append(x)
                df[x] = DEFAULT_CATEGORICAL

        processed_data_classifier = self.preprocessor_classifier.transform(df)
        predictions_classifier = self.model_classifier.predict(
            processed_data_classifier
        )

        return (
            df.assign(
                predicted_class=predictions_classifier.argmax(axis=1),
                consulted_score=predictions_classifier[:, ClassMapping.consulted.value],
                booked_score=predictions_classifier[:, ClassMapping.booked.value],
                score=lambda df: df.consulted_score + df.booked_score,
            ).to_dict(orient="records"),
            errors,
        )


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

    def train(self, df: pd.DataFrame, class_weight: Optional[dict] = None):
        X = self.fit_transform(df)
        y = df[self.target]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, train_size=self.train_size, random_state=42
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
