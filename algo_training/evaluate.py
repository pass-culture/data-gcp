import numpy as np
import pandas as pd
import tensorflow as tf

import tqdm
from scipy.spatial.distance import cosine
from mlflow import pyfunc

from google.cloud import secretmanager
from google.auth.transport.requests import Request
from google.oauth2 import id_token


TYPE_LIST = [
    "ThingType.LIVRE_EDITION",
    "ThingType.INSTRUMENT",
    "EventType.PRATIQUE_ARTISTIQUE",
    "ThingType.AUDIOVISUEL",
    "ThingType.MUSIQUE",
    "EventType.SPECTACLE_VIVANT",
    "ThingType.PRATIQUE_ARTISTIQUE_ABO",
    "EventType.CINEMA",
    "EventType.MUSIQUE",
    "EventType.CONFERENCE_DEBAT_DEDICACE",
    "EventType.MUSEES_PATRIMOINE",
    "ThingType.MUSEES_PATRIMOINE_ABO",
    "ThingType.PRESSE_ABO",
    "ThingType.OEUVRE_ART",
    "ThingType.LIVRE_AUDIO",
    "EventType.JEUX",
    "ThingType.CINEMA_ABO",
    "ThingType.JEUX_VIDEO",
    "ThingType.CINEMA_CARD",
    "ThingType.MUSIQUE_ABO",
    "EventType.ACTIVATION",
    "ThingType.JEUX_VIDEO_ABO",
    "ThingType.SPECTACLE_VIVANT_ABO",
    "ThingType.ACTIVATION",
]


def evaluate(model, storage_path: str):
    pos_data_test = pd.read_csv(f"{storage_path}/pos_data_test.csv", dtype={"user_id": str, "item_id": str})
    pos_data_train = pd.read_csv(f"{storage_path}/pos_data_train.csv", dtype={"user_id": str, "item_id": str})

    metrics = compute_metrics(10, pos_data_train, pos_data_test, model)
    mlflow.log_metrics(metrics)


def get_unexpectedness(booked_type_list, recommended_type_list):
    booked_type_vector_list = [
        [int(booked_type == offer_type) for offer_type in TYPE_LIST]
        for booked_type in booked_type_list
    ]
    recommended_type_vector_list = [
        [int(recommended_type == offer_type) for offer_type in TYPE_LIST]
        for recommended_type in recommended_type_list
    ]

    cosine_sum = 0
    for booked_type_vector in booked_type_vector_list:
        for recommended_type_vector in recommended_type_vector_list:
            cosine_sum += cosine(booked_type_vector, recommended_type_vector)

    return (1 / (len(booked_type_list) * len(recommended_type_list))) * cosine_sum


def compute_metrics(k, pos_data_train, pos_data_test, match_model):
    # map all offers to corresponding types
    offer_type_dict = {}
    unique_offer_types = (
        pos_data_train.groupby(["item_id", "type"]).first().reset_index()
    )
    for item_id, item_type in zip(
        unique_offer_types.item_id.values, unique_offer_types.type.values
    ):
        offer_type_dict[item_id] = item_type

    # Only keep user - item iteractions in pos_data_test, which can be infered from model
    cleaned_pos_data_test = pos_data_test.copy()
    print(
        f"Original number of positive feedbacks in test: {cleaned_pos_data_test.shape[0]}"
    )
    cleaned_pos_data_test = cleaned_pos_data_test.loc[
        lambda df: df.user_id.apply(
            lambda user_id: user_id in pos_data_train.user_id.values
        )
    ]
    print(
        f"Number of positive feedbacks in test after removing users not present in train: {cleaned_pos_data_test.shape[0]}"
    )
    cleaned_pos_data_test = cleaned_pos_data_test.loc[
        lambda df: df.item_id.apply(
            lambda item_id: item_id in pos_data_train.item_id.values
        )
    ]
    print(
        f"Number of positive feedbacks in test after removing offers not present in train: {cleaned_pos_data_test.shape[0]}"
    )

    # Get all offers the model may predict
    all_item_ids = list(set(pos_data_train.item_id.values))
    # Get all users in cleaned test data
    all_test_user_ids = list(set(cleaned_pos_data_test.user_id.values))

    hidden_items_number = 0
    recommended_hidden_items_number = 0
    prediction_number = 0
    recommended_items = []

    user_count = 0
    unexpectedness = []
    serendipity = []
    new_types_ratio = []

    for user_id in tqdm.tqdm(all_test_user_ids):
        user_count += 1

        pos_item_train = pos_data_train[pos_data_train["user_id"] == user_id]
        pos_item_test = cleaned_pos_data_test[
            cleaned_pos_data_test["user_id"] == user_id
        ]

        # remove items in train - they can not be in test set anyway
        items_to_rank = np.setdiff1d(all_item_ids, pos_item_train["item_id"].values)
        booked_offer_types = pos_item_train["type"].values

        # check if any item of items_to_rank is in the test positive feedbacks for this user
        expected = np.in1d(items_to_rank, pos_item_test["item_id"].values)

        repeated_user_id = np.empty_like(items_to_rank)
        repeated_user_id.fill(user_id)

        predicted = match_model.predict(
            [repeated_user_id, items_to_rank], batch_size=4096
        )

        scored_items = sorted(
            [(item_id, score[0]) for item_id, score in zip(items_to_rank, predicted)],
            key=lambda k: k[1],
            reverse=True,
        )[:k]
        recommended_offer_types = [offer_type_dict[item[0]] for item in scored_items]

        user_unexpectedness = get_unexpectedness(
            booked_offer_types, recommended_offer_types
        )
        unexpectedness.append(user_unexpectedness)
        new_types_ratio.append(
            np.mean(
                [
                    int(recommended_offer_type not in booked_offer_types)
                    for recommended_offer_type in recommended_offer_types
                ]
            )
        )

        if np.sum(expected) >= 1:
            recommended_items.extend([item[0] for item in scored_items])
            recommended_items = list(set(recommended_items))

            hidden_items = pos_item_test["item_id"].values
            recommended_hidden_items = [
                item[0] for item in scored_items if item[0] in hidden_items
            ]

            hidden_items_number += len(hidden_items)
            recommended_hidden_items_number += len(recommended_hidden_items)
            prediction_number += 1

            if hidden_items_number > 0:
                user_serendipity = (
                    (len(recommended_hidden_items) / len(hidden_items))
                    * user_unexpectedness
                    * 100
                )
                serendipity.append(user_serendipity)

        metrics = {
            f"recall@{k}": (recommended_hidden_items_number / hidden_items_number) * 100
            if hidden_items_number > 0
            else None,
            f"precision@{k}": (
                recommended_hidden_items_number / (prediction_number * k)
            )
            * 100,
            f"maximal_precision@{k}": (
                cleaned_pos_data_test.shape[0] / (prediction_number * k)
            )
            * 100,
            f"coverage@{k}": (len(recommended_items) / len(all_item_ids)) * 100,
            f"unexpectedness@{k}": np.mean(unexpectedness)
            if len(unexpectedness) > 0
            else None,
            f"new_types_ratio@{k}": np.mean(new_types_ratio)
            if len(new_types_ratio) > 0
            else None,
            f"serendipity@{k}": np.mean(serendipity) if len(serendipity) > 0 else None,
        }

        if user_count % 100 == 0:
            print(metrics)

    return metrics


def load_model(storage_path: str):
    model_path = f"{storage_path}/model"
    loaded_model = tf.saved_model.load(model_path)

    pyfunc_model = pyfunc.load_model(mlflow.get_artifact_uri("model"))
    return loaded_model

def get_secret(secret_id: str):
    client = secretmanager.SecretManagerServiceClient()
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/1"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def connect_remote_mlflow(client_id, env="ehp"):
    """
    Use this function to connect to the mlflow remote server.

    client_id : the oauth iap client id (in 1password)
    """

    MLFLOW_EHP_URI = "https://mlflow-ehp.internal-passculture.app/"
    MLFLOW_PROD_URI = "https://mlflow.internal-passculture.app/"

    client_id = get_secret("mlflow_client_id")

    os.environ["MLFLOW_TRACKING_TOKEN"] = id_token.fetch_id_token(Request(), client_id)
    uri = MLFLOW_PROD_URI if env == "prod" else MLFLOW_EHP_URI
    mlflow.set_tracking_uri(uri)


def main():
    STORAGE_PATH = os.environ.get("STORAGE_PATH", "")
    # model = load_model(STORAGE_PATH)

    ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
    client_id = get_secret("mlflow_client_id", env=ENV_SHORT_NAME)
    connect_remote_mlflow(client_id)
    
    pyfunc_model = pyfunc.load_model(mlflow.get_artifact_uri("model"))
    evaluate(pyfunc_model, STORAGE_PATH)


if __name__ == "__main__":
    main()
