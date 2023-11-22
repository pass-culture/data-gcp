import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
import joblib
import lightgbm as lgb
import typing as t

numeric_features = [
    "user_bookings_count",
    "user_clicks_count",
    "user_favorites_count",
    "user_deposit_remaining_credit",
    "user_is_geolocated",
    "user_iris_x",
    "user_iris_y",
    "offer_user_distance",
    "offer_booking_number",
    "offer_item_score",
    "offer_is_geolocated",
    "offer_stock_price",
    "offer_creation_days",
    "offer_stock_beginning_days",
]

categorical_features = [
    "offer_subcategory_id",
]


class PredictPipeline:
    def __init__(self) -> None:
        self.numeric_features = numeric_features
        self.categorical_features = categorical_features
        self.model = lgb.Booster(model_file="./metadata/model.txt")
        self.preprocessor = joblib.load("./metadata/preproc.joblib")

    def predict(self, input_data: t.List[dict]):
        df = pd.DataFrame(input_data)
        processed_data = self.preprocessor.transform(df)
        z = self.model.predict(processed_data)

        for x, y in zip(input_data, z):
            x["score"] = y
        return input_data


class TrainPipeline:
    def __init__(self, target: str, params: dict = None, verbose: bool = False) -> None:
        self.numeric_features = numeric_features
        self.categorical_features = categorical_features
        self.preprocessor: ColumnTransformer = None
        self.train_size = 0.8
        self.target = target
        if params is None:
            self.params = {
                "objective": "binary",
                "metric": "binary_logloss",
                "boosting_type": "gbdt",
                "is_unbalance": True,
                "num_leaves": 31,
                "learning_rate": 0.05,
                "feature_fraction": 0.9,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "verbose": int(verbose),
            }
        else:
            self.params = params

    def set_pipeline(self):
        numeric_transformer = Pipeline(
            steps=[("imputer", SimpleImputer(strategy="constant", fill_value=-1))]
        )

        categorical_transformer = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
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
        df[self.categorical_features] = df[self.categorical_features].fillna("UNKNOWN")
        df[self.numeric_features] = df[self.numeric_features].fillna(0)
        return self.preprocessor.fit_transform(df)

    def transform(self, input_data):
        df = pd.DataFrame(input_data)
        processed_data = self.preprocessor.transform(df)
        processed_df = pd.DataFrame(processed_data)
        return processed_df.to_numpy()

    def save(self):
        joblib.dump(self.preprocessor, "./metadata/preproc.joblib")
        self.model.save_model("./metadata/model.txt")

    def train(self, df):
        X = self.fit_transform(df)
        y = df[self.target]
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, train_size=self.train_size, random_state=42
        )

        train_data = lgb.Dataset(
            X_train,
            y_train,
            feature_name=self.numeric_features + self.categorical_features,
        )
        test_data = lgb.Dataset(
            X_test,
            y_test,
            feature_name=self.numeric_features + self.categorical_features,
        )

        self.model = lgb.train(
            self.params,
            train_data,
            num_boost_round=10000,
            valid_sets=[train_data, test_data],
            callbacks=[lgb.early_stopping(stopping_rounds=100)],
        )

    def predict(self, df: pd.DataFrame):
        processed_data = self.preprocessor.transform(df)
        df["score"] = self.model.predict(processed_data)
        return df
