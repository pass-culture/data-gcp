import mlflow
import typer

import tensorflow as tf
from loguru import logger

from models.v2.two_tower_model import TwoTowerModel
from tools.data_collect_queries import get_data
from models.v1.match_model import MatchModel
from models.v1.utils import (
    identity_loss,
    predict,
)
from models.v2.utils import (
    load_triplets_dataset,
    MatchModelCheckpoint,
    MLFlowLogging,
)
from utils import (
    get_secret,
    connect_remote_mlflow,
    ENV_SHORT_NAME,
    TRAIN_DIR,
)


N_EPOCHS = 1000
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
    seed: int = typer.Option(
        None,
        help="Seed to fix randomness in pipeline",
    ),
):
    tf.random.set_seed(seed)

    # Load BigQuery data
    train_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="recommendation_training_data"
    )
    validation_data = get_data(
        dataset=f"raw_{ENV_SHORT_NAME}", table_name="recommendation_validation_data"
    )

    user_columns = ["user_id", "user_age"]
    item_columns = ["item_id", "offer_categoryId", "offer_subcategoryid"]
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
    experiment = mlflow.get_experiment_by_name(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        run_uuid = mlflow.active_run().info.run_uuid
        export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/"

        mlflow.log_params(
            params={
                "environment": ENV_SHORT_NAME,
                "embedding_size": embedding_size,
                "batch_size": batch_size,
                "epoch_number": N_EPOCHS,
                "user_count": train_data["user_id"].nunique(),
                "item_count": train_data["item_id"].nunique(),
            }
        )

        two_tower_model = TwoTowerModel(
            user_data=train_data[user_columns].drop_duplicates(),
            item_data=train_data[item_columns].drop_duplicates(),
            embedding_size=embedding_size,
        )
        match_model = MatchModel(two_tower_model.user_layer, two_tower_model.item_layer)

        two_tower_model.compile(loss=identity_loss, optimizer="adam")

        two_tower_model.fit(
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
                ),
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_loss",
                    patience=3,
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

        logger.info("------- TRAINING DONE -------")
        logger.info(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    typer.run(train)
