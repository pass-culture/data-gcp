import implicit
import mlflow
import numpy as np
import pandas as pd
import tensorflow as tf
from tools.v2.mf_reco.preprocess_tools import (
    get_meta_and_sparse,
    add_CS_users_and_get_user_list,
)
from models.v2.mf_reco.matrix_factorization_model import MFModel
from utils import (
    get_secret,
    connect_remote_mlflow,
    STORAGE_PATH,
    ENV_SHORT_NAME,
)

TRAIN_DIR = "/home/airflow/train"


def train(storage_path: str):

    df_train = pd.read_csv(
        f"{storage_path}/clean_data.csv", dtype={"user_id": str, "item_id": str}
    )
    purchases_sparse_train, user_list, item_list = get_meta_and_sparse(df_train)
    feedback_matrix, eac_user_list = add_CS_users_and_get_user_list(
        purchases_sparse_train, storage_path
    )

    user_listwEAC = np.append(eac_user_list, user_list)

    experiment_name = "TEST_algo_training_v2_mf_reco"
    mlflow.set_experiment(experiment_name)
    experiment = mlflow.get_experiment_by_name(experiment_name)

    with mlflow.start_run(experiment_id=experiment.experiment_id):
        mlflow.set_tag("type", "prod_ready")
        hyper_parameters = dict(
            factors=20,
            regularization=0.1,
            subcategories_dim=32,
            iterations=50,
            alpha_val=15,
        )
        mlflow.log_params(hyper_parameters)

        model = implicit.als.AlternatingLeastSquares(
            factors=20, regularization=0.1, iterations=50
        )
        alpha_val = 15
        data_conf = (feedback_matrix * alpha_val).astype("double")
        model.fit(data_conf)

        user_embedding = model.item_factors
        item_embedding = model.user_factors

        MFModel = MFModel(
            list(map(str, user_listwEAC)),
            list(map(str, item_list)),
            user_embedding,
            item_embedding,
        )
        # Need to do a first predict to be able to save the model
        input_test_user = ["eac15" for i in range(4)]
        input_test_items = ["3119148" for i in range(4)]
        MFModel.predict(
            [np.array(input_test_user), np.array(input_test_items)], batch_size=4096
        )
        # Now we can save the trained model
        run_uuid = mlflow.active_run().info.run_uuid
        export_path = f"saved_model/prod_ready/{run_uuid}"
        tf.keras.models.save_model(MFModel, export_path)
        connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
        mlflow.log_artifacts(export_path, "model")
        print("------- TRAINING DONE -------")
        print(mlflow.get_artifact_uri("model"))
    return


if __name__ == "__main__":
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    train(STORAGE_PATH)
