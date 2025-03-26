import json
from collections import OrderedDict

import mlflow
import pandas as pd
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
from commons.mlflow_tools import connect_remote_mlflow, get_mlflow_experiment
from two_towers_model.models.match_model import MatchModel
from two_towers_model.models.two_towers_model import TwoTowersModel
from two_towers_model.utils.callbacks import MLFlowLogging
from two_towers_model.utils.logging import get_git_current_branch

N_EPOCHS = 100
EPOCH_COUNT_PER_SHUFFLE = 5
MIN_DELTA = 0.001
LEARNING_RATE = 0.1
VERBOSE = 2


def setup_gpu_environment():
    """Sets up the GPU environment."""
    if not tf.config.list_physical_devices("GPU"):
        raise Exception("No Cuda device found")

    physical_devices = tf.config.list_physical_devices("GPU")
    for gpu in physical_devices:
        tf.config.experimental.set_memory_growth(gpu, True)
    tf.config.experimental.enable_tensor_float_32_execution(False)


def load_features(config_file_name: str):
    """Loads feature configurations from the specified JSON file."""
    with open(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        features = json.load(config_file, object_pairs_hook=OrderedDict)
    return (
        features["user_embedding_layers"],
        features["item_embedding_layers"],
        features.get("input_prediction_feature", "user_id"),
    )


def convert_df_to_tensor_dict(df: pd.DataFrame) -> dict[str, tf.Tensor]:
    features_dict = {}

    for column in df.columns:
        if df[column].dtype == "object":
            features_dict[column] = df[column].astype(str).values
        else:
            features_dict[column] = df[column].values
    return features_dict


def load_datasets(
    training_table_name: str,
    validation_table_name: str,
    user_columns: list[str],
    item_columns: list[str],
    input_prediction_feature: str,
):
    """Loads and prepares training and validation datasets."""
    logger.info("Loading & processing datasets")

    train_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=training_table_name
    )[user_columns + item_columns].pipe(compute_candidate_sampling_probabilities)

    validation_data = read_from_gcs(
        storage_path=STORAGE_PATH, table_name=validation_table_name
    )[user_columns + item_columns].pipe(compute_candidate_sampling_probabilities)

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

    return train_data, validation_data, train_user_data, train_item_data


def compute_candidate_sampling_probabilities(data: pd.DataFrame):
    dict = data["item_id"].value_counts(normalize=True).to_dict()
    return data.assign(
        **{
            "candidate_sampling_probability": data["item_id"]
            .map(dict)
            .astype("float32")
        }
    )


def build_tf_datasets(
    train_data, validation_data, train_user_data, train_item_data, batch_size
):
    """Builds TensorFlow datasets for training and evaluation."""
    logger.info("Building tf datasets")

    train_dataset = (
        tf.data.Dataset.from_tensor_slices(train_data.pipe(convert_df_to_tensor_dict))
        .cache()
        .shuffle(buffer_size=len(train_data), reshuffle_each_iteration=True)
        .batch(batch_size=batch_size)
        .prefetch(tf.data.AUTOTUNE)
    )

    validation_dataset = (
        tf.data.Dataset.from_tensor_slices(
            validation_data.pipe(convert_df_to_tensor_dict)
        )
        .batch(batch_size=batch_size)
        .cache()
        .prefetch(tf.data.AUTOTUNE)
    )

    user_dataset = (
        tf.data.Dataset.from_tensor_slices(
            train_user_data.pipe(convert_df_to_tensor_dict)
        )
        .batch(batch_size, drop_remainder=False)
        .cache()
        .prefetch(tf.data.AUTOTUNE)
    )

    item_dataset = (
        tf.data.Dataset.from_tensor_slices(
            train_item_data.pipe(convert_df_to_tensor_dict)
        )
        .batch(batch_size, drop_remainder=False)
        .cache()
        .prefetch(tf.data.AUTOTUNE)
    )

    return (
        train_dataset,
        validation_dataset,
        user_dataset,
        item_dataset,
    )


def initialize_mlflow(experiment_name: str, run_name: str):
    """Initializes MLFlow and starts a new run."""
    connect_remote_mlflow()
    experiment = get_mlflow_experiment(experiment_name)
    mlflow.start_run(experiment_id=experiment.experiment_id, run_name=run_name)
    logger.info("Connected to MLFlow")
    run_uuid = mlflow.active_run().info.run_uuid
    with open(f"{MODEL_DIR}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="w") as file:
        file.write(run_uuid)
    return run_uuid


def log_mlflow_params(
    config_file_name: str,
    extra_params: dict,
) -> None:
    """Logs parameters and additional information to MLFlow."""
    mlflow.log_params(
        {
            "environment": ENV_SHORT_NAME,
            "learning_rate": LEARNING_RATE,
            "epoch_number": N_EPOCHS,
            "git_branch": get_git_current_branch(),
            **extra_params,
        }
    )
    mlflow.log_artifact(
        f"{MODEL_DIR}/{CONFIGS_PATH}/{config_file_name}.json", artifact_path="configs"
    )


def train_two_tower_model(
    train_dataset: tf.data.Dataset,
    validation_dataset: tf.data.Dataset,
    two_tower_model: TwoTowersModel,
    training_steps: int,
    validation_steps: int,
    run_uuid: int,
):
    """
    Trains a two-tower model with early stopping and learning rate reduction.

    This function compiles and fits a two-tower model using the provided datasets.
    It configures the model with Adam optimizer and sets up callbacks for
    learning rate reduction, early stopping, and MLFlow logging.

    Args:
        train_dataset (tf.data.Dataset): TensorFlow dataset for training.
        validation_dataset (tf.data.Dataset): TensorFlow dataset for validation.
        two_tower_model (TwoTowersModel): The initialized two-tower model to be trained.
        training_steps (int): Number of steps per epoch during training.
        validation_steps (int): Number of steps for validation.
        run_uuid (str): Unique identifier for the training run, used for logging.

    Returns:
        None: The model is trained in-place.

    Note:
        In this training, we shuffle the training data every EPOCH_COUNT_PER_SHUFFLE epochs. This is done
            in order to activate the optimizer's learning rate reduction and early stopping callbacks.
    """

    # No validation on metrics during training
    two_tower_model.set_task(item_dataset=None)
    two_tower_model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=LEARNING_RATE)
    )

    repeated_train_dataset = train_dataset.repeat()
    two_tower_model.fit(
        repeated_train_dataset,
        epochs=N_EPOCHS,
        steps_per_epoch=training_steps,
        validation_data=validation_dataset,
        validation_steps=validation_steps,
        callbacks=[
            tf.keras.callbacks.ReduceLROnPlateau(
                monitor="val_loss",
                factor=0.5,
                patience=2,
                min_delta=MIN_DELTA,
                verbose=1,
            ),
            tf.keras.callbacks.EarlyStopping(
                monitor="val_loss",
                patience=10,
                min_delta=MIN_DELTA,
                restore_best_weights=True,
                verbose=1,
            ),
            MLFlowLogging(
                export_path=f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/",
            ),
        ],
        verbose=VERBOSE,
    )


def save_model_and_embeddings(
    user_dataset,
    item_dataset,
    two_tower_model,
    train_user_data,
    train_item_data,
    embedding_size,
    run_uuid,
):
    """Saves the trained model and embeddings."""
    logger.info("Predicting final user embeddings")
    user_embeddings = two_tower_model.user_model.predict(user_dataset, verbose=VERBOSE)
    logger.info("Predicting final item embeddings")
    item_embeddings = two_tower_model.item_model.predict(item_dataset, verbose=VERBOSE)

    logger.info("Building and saving the MatchModel")
    match_model = MatchModel(
        user_input=train_user_data.iloc[:, 0].unique(),
        item_ids=train_item_data["item_id"].unique(),
        embedding_size=embedding_size,
    )
    match_model.set_embeddings(
        user_embeddings=user_embeddings, item_embeddings=item_embeddings
    )

    export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/model"
    tf.keras.models.save_model(match_model, export_path)
    mlflow.log_artifacts(export_path, "model")

    logger.info(f"Model and embeddings saved at {export_path}")


def train(
    experiment_name: str = typer.Option(..., help="MLFlow experiment name"),
    config_file_name: str = typer.Option(
        ..., help="Name of the config file containing feature informations"
    ),
    batch_size: int = typer.Option(..., help="Batch size of training"),
    embedding_size: int = typer.Option(..., help="Item & User embedding size"),
    seed: int = typer.Option(None, help="Seed to fix randomness in pipeline"),
    training_table_name: str = typer.Option(
        "recommendation_training_data", help="BigQuery table containing training data"
    ),
    validation_table_name: str = typer.Option(
        "recommendation_validation_data",
        help="BigQuery table containing validation data",
    ),
    run_name: str = typer.Option(None, help="Name of the MLflow run if set"),
):
    setup_gpu_environment()
    tf.random.set_seed(seed)
    run_uuid = initialize_mlflow(experiment_name, run_name)

    user_features_config, item_features_config, input_prediction_feature = (
        load_features(config_file_name)
    )

    user_columns = list(user_features_config.keys())
    item_columns = list(item_features_config.keys())

    train_data, validation_data, train_user_data, train_item_data = load_datasets(
        training_table_name,
        validation_table_name,
        user_columns=user_columns,
        item_columns=item_columns,
        input_prediction_feature=input_prediction_feature,
    )

    train_dataset, validation_dataset, user_dataset, item_dataset = build_tf_datasets(
        train_data, validation_data, train_user_data, train_item_data, batch_size
    )
    log_mlflow_params(
        config_file_name=config_file_name,
        extra_params={
            "embedding_size": embedding_size,
            "batch_size": batch_size,
            "user_count": len(train_user_data),
            "user_feature_count": len(user_features_config.keys()),
            "item_count": len(train_item_data),
            "item_feature_count": len(item_features_config.keys()),
            "epoch_count_per_shuffle": EPOCH_COUNT_PER_SHUFFLE,
        },
    )

    logger.info("Create Model")
    two_tower_model = TwoTowersModel(
        data=train_data,
        user_features_config=user_features_config,
        item_features_config=item_features_config,
        user_columns=user_columns,
        item_columns=item_columns,
        embedding_size=embedding_size,
    )

    logger.info("Training Model")
    training_steps = max((len(train_data) / EPOCH_COUNT_PER_SHUFFLE) // batch_size, 1)
    validation_steps = max(len(validation_data) // batch_size, 1)
    train_two_tower_model(
        train_dataset,
        validation_dataset,
        two_tower_model,
        training_steps=training_steps,
        validation_steps=validation_steps,
        run_uuid=run_uuid,
    )

    logger.info("Compute embeddings and save Model")
    save_model_and_embeddings(
        user_dataset,
        item_dataset,
        two_tower_model,
        train_user_data,
        train_item_data,
        embedding_size,
        run_uuid,
    )


if __name__ == "__main__":
    typer.run(train)
