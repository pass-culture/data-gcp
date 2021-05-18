import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras import layers
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Embedding, Flatten, Input, Dense
from tensorflow.keras.layers import Lambda, Dot
from tensorflow.keras.regularizers import l2
from triplet_model import TripletModel
from sklearn.metrics import roc_auc_score

from match_model import MatchModel
from margin_loss import MarginLoss

# Get secret
from google.cloud import secretmanager_v1

# Get token for connection
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import os
import mlflow


def train(storage_path: str):

    # fetch data
    bookings = pd.read_csv(f"{storage_path}/clean_data.csv")
    # bookings = pd.read_csv(f"{storage_path}/pos_data_train.csv")

    pos_data_test = pd.read_csv(f"{storage_path}/pos_data_test.csv")
    pos_data_train = pd.read_csv(f"{storage_path}/pos_data_train.csv")

    n_users = len(set(bookings.user_id.values))
    n_items = len(set(bookings.item_id.values))

    triplet_model = TripletModel(n_users, n_items, latent_dim=64, l2_reg=1e-6)
    match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)

    with mlflow.start_run():
        MODEL_DATA_PATH = "tf_bpr_string_input_5_months_reg_0"
        EMBEDDING_SIZE = 64
        L2_REG = 0  # 1e-6

        n_epochs = 20
        batch_size = 32

        triplet_model = TripletModel(
            user_ids, item_ids, latent_dim=EMBEDDING_SIZE, l2_reg=L2_REG
        )
        match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)

        fake_y = np.array(["1"] * pos_data_train["user_id"].shape[0], dtype=object)
        evaluation_fake_train = np.array(
            ["1"] * pos_data_eval["user_id"].shape[0], dtype=object
        )
        triplet_model.compile(loss=identity_loss, optimizer="adam")

        best_eval = 1

        train = []
        evaluation = []
        runned_epochs = 0
        for i in range(n_epochs):
            # Sample new negatives to build different triplets at each epoch
            triplet_inputs = sample_triplets(pos_data_train, item_ids)
            evaluation_triplet_inputs = sample_triplets(pos_data_eval, item_ids)
            # ds = tf.data.Dataset.from_tensor_slices(triplet_inputs)

            # Fit the model incrementally by doing a single pass over the
            # sampled triplets.
            print(f"Training epoch {i}")
            train_result = triplet_model.fit(
                x=triplet_inputs, y=fake_y, shuffle=True, batch_size=64, epochs=1
            )
            train.append(train_result.history["loss"][0])
            print(train)
            print(f"Evaluation epoch {i}")
            eval_result = triplet_model.evaluate(
                x=evaluation_triplet_inputs, y=evaluation_fake_train, batch_size=64
            )
            # LOG into MLFLOW
            mlflow.log_metric(key="eval_result", value=eval_result, step=i)

            evaluation.append(eval_result)
            print(evaluation)
            runned_epochs += 1
            if eval_result < best_eval:
                tf.saved_model.save(match_model, f"{MODEL_DATA_PATH}/tf_bpr_{i}epochs")
                best_eval = eval_result

        # TRAINING CURVES
        # plt.plot(list(range(runned_epochs)), train[:runned_epochs], label="Train Loss")
        # plt.plot(
        #     list(range(runned_epochs)), evaluation[:runned_epochs], label="Evaluation Loss"
        # )
        # plt.xlabel("Epoch")
        # plt.ylabel("Losses")
        # plt.legend()
        # plt.savefig(f"{MODEL_DATA_PATH}/learning_curves.png")

        metrics = compute_metrics(10, pos_data_train, pos_data_test, match_model)

        print(metrics)
        save_dict_to_path(metrics, f"{MODEL_DATA_PATH}/metrics.json")


def identity_loss(y_true, y_pred):
    """Ignore y_true and return the mean of y_pred

    This is a hack to work-around the design of the Keras API that is
    not really suited to train networks with a triplet loss by default.
    """
    return tf.reduce_mean(y_pred)


def sample_triplets(pos_data, item_ids):
    """Sample negatives at random"""

    user_ids = pos_data["user_id"].values
    pos_item_ids = pos_data["item_id"].values
    neg_item_ids = np.array(random.choices(item_ids, k=len(user_ids)), dtype=object)

    return [user_ids, pos_item_ids, neg_item_ids]


def save_model(storage_path: str, model_name: str):
    model_path = f"{storage_path}/{model_name}"
    tf.saved_model.save(match_model, model_path)
    loaded = tf.saved_model.load(model_path)


def get_secret(secret_name: str):
    client = secretmanager_v1.SecretManagerServiceClient()
    secret = client.get_secret(request={"mlflow_client_id": "mlflow_client_id"})
    return secret


def connect_remote_mlflow(client_id, env="dev"):
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
    ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "ehp")
    train(STORAGE_PATH)


if __name__ == "__main__":
    main()
