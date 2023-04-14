import mlflow
import tensorflow as tf
import typer
from loguru import logger

from baseline_model.models.baseline_model import BaselineModel
from baseline_model.models.match_model import MatchModel
from utils.callbacks import MLFlowLogging
from utils.constants import (
    ENV_SHORT_NAME,
    MODEL_DIR,
    STORAGE_PATH,
    TRAIN_DIR,
    MLFLOW_RUN_ID_FILENAME,
)
from utils.data_collect_queries import read_from_gcs
from utils.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment
from utils.secrets_utils import get_secret

N_EPOCHS = 100
MIN_DELTA = 0.001  # Minimum change in the accuracy before a callback is called
LEARNING_RATE = 0.1
VERBOSE = 2 if ENV_SHORT_NAME == "prod" else 1


def train(
    experiment_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    ),
    batch_size: int = typer.Option(
        ...,
        help="Batch size of training",
    ),
    validation_steps_ratio: float = typer.Option(
        ...,
        help="Ratio of the total validation steps that will be processed at evaluation",
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

    # Load data
    logger.info("Loading & processing datasets")
    train_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=training_table_name
    )[["user_id", "item_id"]].astype(str)
    logger.info(f"Train shape {train_data.shape[0]}")

    validation_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=validation_table_name
    )[["user_id", "item_id"]].astype(str)

    logger.info(f"Train shape {validation_data.shape[0]}")

    train_user_ids = train_data["user_id"].unique()
    train_item_ids = train_data["item_id"].unique()

    # Build tf datasets
    logger.info("Building tf datasets")

    train_dataset = (
        tf.data.Dataset.from_tensor_slices(train_data.values)
        .batch(batch_size=batch_size)
        .map(lambda x: tf.transpose(x))
    )
    validation_dataset = (
        tf.data.Dataset.from_tensor_slices(validation_data.values)
        .batch(batch_size=batch_size)
        .map(lambda x: tf.transpose(x))
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

        export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/"

        mlflow.log_params(
            params={
                "environment": ENV_SHORT_NAME,
                "embedding_size": embedding_size,
                "batch_size": batch_size,
                "epoch_number": N_EPOCHS,
                "user_count": len(train_user_ids),
                "item_count": len(train_item_ids),
            }
        )

        logger.info("Building the BaselineModel")

        baseline_model = BaselineModel(
            user_ids=train_user_ids,
            item_ids=train_item_ids,
            embedding_size=embedding_size,
        )

        baseline_model.compile(
            optimizer=tf.keras.optimizers.Adagrad(learning_rate=LEARNING_RATE),
        )

        # Divide the total validation steps by a ratio to speed up training
        validation_steps = max(
            int((validation_data.shape[0] // batch_size) * validation_steps_ratio), 1
        )
        logger.info(f"Total validation steps {validation_steps}")

        logger.info("Fit")

        baseline_model.fit(
            train_dataset,
            epochs=N_EPOCHS,
            validation_data=validation_dataset,
            validation_steps=validation_steps,
            callbacks=[
                tf.keras.callbacks.ReduceLROnPlateau(
                    monitor="val_factorized_top_k/top_50_categorical_accuracy",
                    factor=0.1,
                    patience=5,
                    min_delta=MIN_DELTA,
                    verbose=1,
                ),
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_factorized_top_k/top_50_categorical_accuracy",
                    patience=10,
                    min_delta=MIN_DELTA,
                    verbose=1,
                ),
                MLFlowLogging(
                    client_id=client_id,
                    env=ENV_SHORT_NAME,
                    export_path=export_path,
                ),
            ],
            verbose=VERBOSE,
        )

        logger.info("Predicting final user embeddings")
        user_embeddings = baseline_model.user_model.predict(train_user_ids)
        logger.info("Predicting final item embeddings")
        item_embeddings = baseline_model.item_model.predict(train_item_ids)

        logger.info("Building and saving the MatchModel")
        match_model = MatchModel(
            user_ids=train_user_ids.tolist(),
            item_ids=train_item_ids.tolist(),
            embedding_size=embedding_size,
        )
        match_model.set_embeddings(
            user_embeddings=user_embeddings, item_embeddings=item_embeddings
        )

        tf.keras.models.save_model(match_model, export_path + "model")
        mlflow.log_artifacts(export_path + "model", "model")

        logger.info("------- TRAINING DONE -------")
        logger.info(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    typer.run(train)
