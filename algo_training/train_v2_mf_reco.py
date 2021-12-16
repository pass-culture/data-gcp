import implicit
import mlflow
from utils.v2.mf_reco.preprocess_tools import (
    get_meta_and_sparse,
    csr_vappend,
    add_CS_users_and_get_user_list,
)

from models.v2.mf_reco.matrix_factorization_model import MFmodel

TRAIN_DIR = "/home/airflow/train"


def train(storage_path: str):

    df_train = pd.read_csv("./df_train_0104_1508.csv")
    purchases_sparse_train, user_list, item_list = get_meta_and_sparse(df_train)
    feedback_matrix, eac_user_list = add_CS_users_and_get_user_list(
        purchases_sparse_train, storage_path
    )

    user_list = eac_user_list.append(user_list)

    experiment_name = "algo_training_v2_mf_reco"
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
        data_conf = (purchases_sparse_train * alpha_val).astype("double")
        model.fit(data_conf)

        user_embedding = model.factors()
        item_embedding = model.factors()

        run_uuid = mlflow.active_run().info.run_uuid
        export_path = f"saved_model/prod_ready/{run_uuid}"
        tf.keras.models.save_model(deep_match_model, export_path)
        connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
        mlflow.log_artifacts(export_path, "model")
        print("------- TRAINING DONE -------")
        print(mlflow.get_artifact_uri("model"))
    return
