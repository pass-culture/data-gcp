import numpy as np
from sklearn.externals import joblib
from sklearn.dummy import DummyRegressor
from sklearn.pipeline import Pipeline


def generate_and_save_constant_model(model_path):
    X = np.array([1.0])
    y = np.array([1.0])

    pipeline = Pipeline([("model", DummyRegressor(strategy="constant", constant=1))])

    pipeline.fit(X, y)

    joblib.dump(pipeline, model_path)


if __name__ == "__main__":
    generate_and_save_constant_model("ai_platform/model.joblib")
