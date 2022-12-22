import mlflow
import warnings

import typer
from pandas.errors import DtypeWarning

from tools.data_collect_queries import get_data

import tensorflow as tf

from models.v1.match_model import MatchModel
from models.v1.triplet_model import TripletModel
from models.v1.utils import (
    identity_loss,
    predict,
    load_triplets_dataset,
    MatchModelCheckpoint,
    MLFlowLogging,
)
from utils import (
    get_secret,
    connect_remote_mlflow,
    STORAGE_PATH,
    ENV_SHORT_NAME,
    EXPERIMENT_NAME,
    TRAIN_DIR,
)


L2_REG = 0
N_EPOCHS = 20 if ENV_SHORT_NAME == "prod" else 10
VERBOSE = 0 if ENV_SHORT_NAME == "prod" else 1
LOSS_CUTOFF = 0.005


def train(
    experiment_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    ),
    batch_size: int = typer.Option(
        ...,
        help="Batch size of training",
    ),
    embedding_size: int = typer.Option(
        ...,
        help="Item & User embedding size",
    ),
):
    # TODO: training_data_clicks should be called data_clicks
    train_data = get_data(dataset="raw_dev", table_name="training_data").astype(
        dtype={"count": int}
    )
    validation_data = get_data(dataset="raw_dev", table_name="validation_data").astype(
        dtype={"count": int}
    )

    training_user_ids = train_data["user_id"].unique()
    training_item_ids = train_data["item_id"].unique()

    # Create tf datasets
    train_dataset = load_triplets_dataset(train_data, item_ids=training_item_ids)
    validation_dataset = load_triplets_dataset(
        validation_data, item_ids=training_item_ids
    )

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)

    experiment = mlflow.get_experiment_by_name(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        run_uuid = mlflow.active_run().info.run_uuid
        export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/tf_reco/"

        mlflow.log_params(
            params={
                "environment": ENV_SHORT_NAME,
                "embedding_size": embedding_size,
                "batch_size": batch_size,
                "l2_regularization": L2_REG,
                "epoch_number": N_EPOCHS,
                "user_count": len(training_user_ids),
                "item_count": len(training_item_ids),
            }
        )

        triplet_model = TripletModel(
            user_ids=training_user_ids,
            item_ids=training_item_ids,
            latent_dim=embedding_size,
            l2_reg=L2_REG,
        )
        match_model = MatchModel(triplet_model.user_layer, triplet_model.item_layer)
        predict(match_model)

        triplet_model.compile(loss=identity_loss, optimizer="adam")

        triplet_model.fit(
            data=train_dataset,
            epochs=N_EPOCHS,
            batch_size=batch_size,
            steps_per_epoch=len(train_data) // batch_size,
            validation_data=validation_dataset,
            verbose=VERBOSE,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_loss",
                    min_delta=LOSS_CUTOFF,
                ),
                MatchModelCheckpoint(
                    match_model=match_model,
                    filepath=export_path,
                ),
                MLFlowLogging(
                    client_id=client_id,
                    env=ENV_SHORT_NAME,
                    export_path=export_path,
                ),
            ],
        )

        print("------- TRAINING DONE -------")
        print(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    typer.run(train)
