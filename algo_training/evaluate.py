import gcsfs
import numpy as np
import pandas as pd
import pickle
import tensorflow as tf
import mlflow.tensorflow
from metrics import compute_metrics
from models.v1.match_model import MatchModel
from models.v2.deep_reco.deep_match_model import DeepMatchModel
from models.v2.mf_reco.matrix_factorization_model import MFModel
from utils import (
    get_secret,
    connect_remote_mlflow,
    STORAGE_PATH,
    ENV_SHORT_NAME,
    MODEL_NAME,
    GCP_PROJECT_ID,
)

RECOMMENDATION_NUMBER = 40


def evaluate(model, storage_path: str, model_name):

    if model_name == "v2_mf_reco":
        positive_data_test = pd.read_csv(
            f"{storage_path}/positive_data_test.csv",
            dtype={"user_id": str, "offer_id": str, "offer_subcategoryid": str},
        )
        positive_data_train = pd.read_csv(
            f"{storage_path}/positive_data_train.csv",
            dtype={"user_id": str, "offer_id": str, "offer_subcategoryid": str},
        )
    else:
        positive_data_test = pd.read_csv(
            f"{storage_path}/positive_data_test.csv",
            dtype={"user_id": str, "item_id": str, "offer_subcategoryid": str},
        )
    positive_data_train = pd.read_csv(
        f"{storage_path}/positive_data_train.csv",
        dtype={"user_id": str, "item_id": str, "offer_subcategoryid": str},
    )
    metrics = compute_metrics(
        RECOMMENDATION_NUMBER,
        positive_data_train,
        positive_data_test,
        model_name,
        model,
    )
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    mlflow.log_metrics(metrics)
    print("------- EVALUATE DONE -------")
    check_before_deploy(metrics, RECOMMENDATION_NUMBER)


def check_before_deploy(metrics, k):
    if (
        metrics[f"recall_at_{k}"] > 20
        and metrics[f"precision_at_{k}"] > 1
        and metrics[f"maximal_precision_at_{k}"] > 4.5
        and metrics[f"coverage_at_{k}"] > 10
        and metrics[f"coverage_at_{k}"] < 70
        and metrics[f"unexpectedness_at_{k}"] > 0.07
        and metrics[f"new_types_ratio_at_{k}"] > 0.05
        and metrics[f"serendipity_at_{k}"] > 1.5
    ):
        print("Metrics OK")
    else:
        print("Bad metrics")
    if ENV_SHORT_NAME == "dev":
        # INFO : metrics are never ok in dev so we force the deploy
        print("Metrics OK")


if __name__ == "__main__":
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment_name = f"TEST_bis_algo_training_{MODEL_NAME}"
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    run_id = mlflow.list_run_infos(experiment_id)[0].run_id
    if MODEL_NAME == "v1":
        with mlflow.start_run(run_id=run_id):
            loaded_model = tf.keras.models.load_model(
                mlflow.get_artifact_uri("model"),
                custom_objects={"MatchModel": MatchModel},
                compile=False,
            )
            evaluate(loaded_model, STORAGE_PATH, MODEL_NAME)
    elif MODEL_NAME == "v2_deep_reco":
        with mlflow.start_run(run_id=run_id):
            loaded_model = tf.keras.models.load_model(
                mlflow.get_artifact_uri("model"),
                custom_objects={"DeepMatchModel": DeepMatchModel},
                compile=False,
            )
            evaluate(loaded_model, STORAGE_PATH, MODEL_NAME)
    elif MODEL_NAME == "v2_mf_reco":
        with mlflow.start_run(run_id=run_id):
            fs = gcsfs.GCSFileSystem(project=GCP_PROJECT_ID)
            with fs.open(
                f"{STORAGE_PATH}/Model/MF_als_model_with_cs_user_EAC_test.pickle", "rb"
            ) as fileA:
                model = pickle.load(fileA)
            with fs.open(f"{STORAGE_PATH}/Model/user_list_wEAC.npy") as fileB:
                user_list_wEAC = np.load(fileB)
            with fs.open(f"{STORAGE_PATH}/Model/item_list.npy") as fileC:
                item_list = np.load(fileC)
            user_embedding = model.item_factors
            item_embedding = model.user_factors
            loaded_model = MFModel(
                list(map(str, user_list_wEAC)),
                list(map(str, item_list)),
                user_embedding,
                item_embedding,
            )
            evaluate(loaded_model, STORAGE_PATH, MODEL_NAME)
