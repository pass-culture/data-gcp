import json

import mlflow
import tensorflow as tf
import typer
from loguru import logger

from commons.constants import (
    CONFIGS_PATH,
    ENV_SHORT_NAME,
    MLFLOW_RUN_ID_FILENAME,
    MODEL_DIR,
    STORAGE_PATH,
    TRAIN_DIR,
)
from commons.data_collect_queries import read_from_gcs
from commons.mlflow_tools import (
    connect_remote_mlflow,
    get_mlflow_experiment,
)
from two_towers_model.models.match_model import MatchModel
from two_towers_model.models.two_towers_model import TwoTowersModel
from two_towers_model.utils.callbacks import MLFlowLogging

N_EPOCHS = 100
MIN_DELTA = 0.001  # Minimum change in the accuracy before a callback is called
LEARNING_RATE = 0.1
VERBOSE = 1 if ENV_SHORT_NAME == "prod" else 1


def train(
    experiment_name: str = typer.Option(
        ...,
        help="MLFlow experiment name",
    ),
    config_file_name: str = typer.Option(
        ...,
        help="Name of the config file containing feature informations",
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
    if not tf.config.list_physical_devices("GPU"):
        raise Exception("No Cuda device found")

    tf.random.set_seed(seed)

    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file)
        user_features_config, item_features_config, input_prediction_feature = (
            features["user_embedding_layers"],
            features["item_embedding_layers"],
            features.get("input_prediction_feature", "user_id"),
        )

    # Load data
    logger.info("Loading & processing datasets")

    user_columns = list(user_features_config.keys())
    item_columns = list(item_features_config.keys())
    # We ensure that the datasets contains the features in the correct order (user_id, ..., item_id, ...)
    train_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=training_table_name
    )[user_columns + item_columns].astype(str)
    validation_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=validation_table_name
    )[user_columns + item_columns].astype(str)

    train_user_data = (
        train_data[user_columns]
        .drop_duplicates(subset=[input_prediction_feature])
        .reset_index(drop=True)
    )
    train_item_data = (
        train_data[item_columns]
        .drop_duplicates(subset=["item_id"])
        .reset_index(drop=True)
    )

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

    user_dataset = (
        tf.data.Dataset.from_tensor_slices(train_user_data.values)
        .batch(batch_size=batch_size, drop_remainder=False)
        .map(lambda x: tf.transpose(x))
        .cache()
    )

    item_dataset = (
        tf.data.Dataset.from_tensor_slices(train_item_data.values)
        .batch(batch_size=batch_size, drop_remainder=False)
        .map(lambda x: tf.transpose(x))
        .cache()
    )

    # Connect to MLFlow
    connect_remote_mlflow()
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
                "user_count": len(train_user_data),
                "user_feature_count": len(user_features_config.keys()),
                "item_count": len(train_item_data),
                "item_feature_count": len(item_features_config.keys()),
            }
        )

        logger.info("Building the TwoTowersModel")

        two_tower_model = TwoTowersModel(
            data=train_data,
            user_features_config=user_features_config,
            item_features_config=item_features_config,
            items_dataset=item_dataset,
            embedding_size=embedding_size,
        )

        two_tower_model.compile(
            optimizer=tf.keras.optimizers.Adagrad(learning_rate=LEARNING_RATE),
        )

        # Divide the total validation steps by a ration to speed up training
        validation_steps = min(
            max(
                int((validation_data.shape[0] // batch_size) * validation_steps_ratio),
                1,
            ),
            10,
        )
        # TODO https://github.com/tensorflow/recommenders/issues/388
        logger.info(f"Validation steps {validation_steps}")
        two_tower_model.fit(
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
                    export_path=export_path,
                ),
            ],
            verbose=VERBOSE,
        )

        logger.info("Predicting final user embeddings")
        user_embeddings = two_tower_model.user_model.predict(user_dataset)
        logger.info("Predicting final item embeddings")
        item_embeddings = two_tower_model.item_model.predict(item_dataset)
        logger.info("Normalizing embeddings...")
        user_embeddings = tf.math.l2_normalize(user_embeddings, axis=1)
        item_embeddings = tf.math.l2_normalize(item_embeddings, axis=1)
        logger.info("Building and saving the MatchModel")
        match_model = MatchModel(
            user_input=train_user_data[input_prediction_feature].unique(),
            item_ids=train_item_data["item_id"].unique(),
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
