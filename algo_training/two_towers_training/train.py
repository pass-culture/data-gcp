import json
import os

import mlflow
import typer

import tensorflow as tf
from loguru import logger
import pandas as pd

from models.match_model import TwoTowersMatchModel
from models.two_towers_model import TwoTowersModel
from tools.constants import (
    CONFIG_FEATURES_PATH,
    ENV_SHORT_NAME,
    TRAIN_DIR,
    STORAGE_PATH,
    MLFLOW_RUN_ID_FILENAME,
)
from tools.tensorflow_tools import build_dict_dataset, MLFlowLogging
from tools.utils import save_pca_representation, get_secret, connect_remote_mlflow

N_EPOCHS = 20
MIN_DELTA = 0.002  # Minimum change in the accuracy before a callback is called
LEARNING_RATE = 0.1
VERBOSE = 2


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

    # Load data
    logger.info("Loading & processing datasets")

    train_data = pd.read_csv(
        f"{STORAGE_PATH}/positive_data_train.csv",
    ).astype(str)
    validation_data = pd.read_csv(
        f"{STORAGE_PATH}/positive_data_eval.csv",
    ).astype(str)

    with open(
        CONFIG_FEATURES_PATH + f"/{config_file_name}.json", mode="r", encoding="utf-8"
    ) as config_file:
        features = json.load(config_file)
        user_features_config, item_features_config = (
            features["user_embedding_layers"],
            features["item_embedding_layers"],
        )

    user_columns = list(user_features_config.keys())
    item_columns = list(item_features_config.keys())
    train_user_data = train_data[user_columns].drop_duplicates(subset=["user_id"])
    train_item_data = train_data[item_columns].drop_duplicates(subset=["item_id"])

    # Build tf datasets
    logger.info("Building tf datasets")

    train_dataset = build_dict_dataset(
        train_data,
        feature_names=user_columns + item_columns,
        batch_size=batch_size,
        seed=seed,
    )
    validation_dataset = build_dict_dataset(
        validation_data,
        feature_names=user_columns + item_columns,
        batch_size=batch_size,
        seed=seed,
    )

    user_dataset = build_dict_dataset(
        train_user_data,
        feature_names=user_columns,
        batch_size=batch_size,
        seed=seed,
    )
    item_dataset = build_dict_dataset(
        train_item_data,
        feature_names=item_columns,
        batch_size=batch_size,
        seed=seed,
    )

    # Connect to MLFlow
    client_id = get_secret("mlflow_client_id")
    connect_remote_mlflow(client_id, env=ENV_SHORT_NAME)
    experiment = mlflow.get_experiment_by_name(experiment_name)
    with mlflow.start_run(experiment_id=experiment.experiment_id):
        logger.info("Connected to MLFlow")

        run_uuid = mlflow.active_run().info.run_uuid
        export_path = f"{TRAIN_DIR}/{ENV_SHORT_NAME}/{run_uuid}/"
        os.makedirs(export_path, exist_ok=True)
        with open(f"{export_path}/{MLFLOW_RUN_ID_FILENAME}.txt", mode="w") as file:
            file.write(run_uuid)

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

        two_tower_model.fit(
            train_dataset,
            epochs=N_EPOCHS,
            validation_data=validation_dataset,
            verbose=VERBOSE,
            callbacks=[
                tf.keras.callbacks.ReduceLROnPlateau(
                    monitor="val_factorized_top_k/top_100_categorical_accuracy",
                    factor=0.1,
                    patience=2,
                    min_delta=MIN_DELTA,
                    verbose=1,
                ),
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_factorized_top_k/top_100_categorical_accuracy",
                    patience=3,
                    min_delta=MIN_DELTA,
                    verbose=1,
                ),
                MLFlowLogging(
                    client_id=client_id,
                    env=ENV_SHORT_NAME,
                    export_path=export_path,
                ),
            ],
        )

        logger.info("Building and saving the MatchModel")

        user_embeddings = two_tower_model.user_model.predict(user_dataset)
        item_embeddings = two_tower_model.item_model.predict(item_dataset)

        match_model = TwoTowersMatchModel(
            user_ids=train_user_data["user_id"].unique(),
            user_embeddings=user_embeddings,
            item_ids=train_item_data["item_id"].unique(),
            item_embeddings=item_embeddings,
            embedding_size=embedding_size,
        )
        tf.keras.models.save_model(match_model, export_path + "model")
        mlflow.log_artifacts(export_path + "model", "model")

        # Export the PCA representations of the item embeddings
        os.makedirs(export_path + "pca_plots", exist_ok=True)
        pca_representations_path = export_path + "pca_plots/"
        save_pca_representation(
            loaded_model=match_model,
            item_data=train_item_data,
            figures_folder=pca_representations_path,
        )
        mlflow.log_artifacts(export_path + "pca_plots", "pca_plots")

        logger.info("------- TRAINING DONE -------")
        logger.info(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    typer.run(train)
