import mlflow
import pandas as pd
import tensorflow as tf
import typer
from loguru import logger

from triplet_model.models.match_model import MatchModel
from triplet_model.models.triplet_model import TripletModel
from triplet_model.utils.callbacks import MatchModelCheckpoint, MLFlowLogging
from triplet_model.utils.dataset_utils import load_triplets_dataset
from triplet_model.utils.model_utils import predict, identity_loss
from utils.constants import (
    STORAGE_PATH,
    ENV_SHORT_NAME,
    TRAIN_DIR,
    MODEL_DIR,
    MLFLOW_RUN_ID_FILENAME,
)
from utils.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment
from utils.secrets_utils import get_secret

L2_REG = 0
N_EPOCHS = 1000
VERBOSE = 2
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
    seed: int = typer.Option(
        None,
        help="Seed to fix randomness in pipeline",
    ),
    training_table_name: str = typer.Option(
        "recommendation_training_data", help="BigQuery table containing training data"
    ),
    validation_table_name: str = typer.Option(
        "recommendation_validation_data",
        help="BigQuery table containing validation data",
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
):
    tf.random.set_seed(seed)

    train_data = pd.read_csv(
        f"{STORAGE_PATH}/{training_table_name}.csv",
    ).astype(str)
    validation_data = pd.read_csv(
        f"{STORAGE_PATH}/{validation_table_name}.csv",
    ).astype(str)

    training_user_ids = train_data["user_id"].unique()
    training_item_ids = train_data["item_id"].unique()

    user_columns = ["user_id"]
    item_columns = ["item_id"]
    # Create tf datasets
    train_dataset = load_triplets_dataset(
        train_data,
        user_columns=user_columns,
        item_columns=item_columns,
        batch_size=batch_size,
    )
    validation_dataset = load_triplets_dataset(
        validation_data,
        user_columns=user_columns,
        item_columns=item_columns,
        batch_size=batch_size,
    )

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment = get_mlflow_experiment(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name):
        logger.info("Connected to MLFlow")

        run_uuid = mlflow.active_run().info.run_uuid
        # TODO: store the run_uuid in STORAGE_PATH (last try raised FileNotFoundError)
        with open(f"{MODEL_DIR}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="w") as file:
            file.write(run_uuid)

        # used by sim_offers model
        export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/"

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
            train_dataset,
            epochs=N_EPOCHS,
            validation_data=validation_dataset,
            verbose=VERBOSE,
            callbacks=[
                tf.keras.callbacks.ReduceLROnPlateau(
                    monitor="val_loss",
                    factor=0.1,
                    patience=2,
                    min_delta=LOSS_CUTOFF,
                    verbose=1,
                ),
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_loss", patience=3, min_delta=LOSS_CUTOFF, verbose=1
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

        logger.info("------- TRAINING DONE -------")
        logger.info(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    typer.run(train)
