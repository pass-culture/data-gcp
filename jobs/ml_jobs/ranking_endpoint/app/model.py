import typing as t

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

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
        self.model = lgb.Booster(model_file="./metadata/model.txt")
        self.preprocessor = joblib.load("./metadata/preproc.joblib")

    def predict(self, input_data: t.List[dict]):
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

        processed_data = self.preprocessor.transform(df)
        z = self.model.predict(processed_data)

        for x, y in zip(input_data, z):
            x["score"] = y
        return input_data, errors


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

    def save(self):
        joblib.dump(self.preprocessor, "./metadata/preproc.joblib")
        self.model.save_model("./metadata/model.txt")

    def train(self, df: pd.DataFrame, class_weight: dict):
        X = self.fit_transform(df)
        y = df[self.target]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, train_size=self.train_size, random_state=42
        )

        train_data = lgb.Dataset(
            X_train,
            y_train,
            feature_name=self.numeric_features + self.categorical_features,
            weight=np.array([class_weight[label] for label in y_train]),
        )
        test_data = lgb.Dataset(
            X_test,
            y_test,
            feature_name=self.numeric_features + self.categorical_features,
            weight=np.array([class_weight[label] for label in y_test]),
        )

        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=50_000,
            valid_sets=[train_data, test_data],
            callbacks=[lgb.early_stopping(stopping_rounds=200)],
        )

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        processed_data = self.preprocessor.transform(df)
        probabilities = self.model.predict(processed_data)

        # Assuming predictions are probabilities, get the class with highest probability
        predicted_classes = probabilities.argmax(axis=1)
        df["predicted_class"] = predicted_classes

        # Store probabilities if needed
        for i in range(probabilities.shape[1]):
            df[f"prob_class_{i}"] = probabilities[:, i]

        return df
