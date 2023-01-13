import os

import mlflow
import typer

import tensorflow as tf
from loguru import logger
import pandas as pd

from models.v2.two_towers.two_towers_match_model import TwoTowersMatchModel
from models.v2.two_towers.two_towers_model import TwoTowersModel
from models.v1.utils import (
    identity_loss,
)
from models.v2.utils import (
    load_triplets_dataset,
    MLFlowLogging,
    save_pca_representation,
)
from utils import (
    get_secret,
    connect_remote_mlflow,
    ENV_SHORT_NAME,
    TRAIN_DIR,
    STORAGE_PATH,
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
    train_data = pd.read_csv(
        f"{STORAGE_PATH}/positive_data_train.csv",
    ).astype(str)
    validation_data = pd.read_csv(
        f"{STORAGE_PATH}/positive_data_eval.csv",
    ).astype(str)

    user_layer_infos = {
        "user_id": {"type": "string", "feature_latent_dim": 128},
        "user_age": {"type": "int", "feature_latent_dim": 16},
        "user_postal_code": {"type": "string", "feature_latent_dim": 8},
        "user_activity": {"type": "string", "feature_latent_dim": 8},
        "user_booking_cnt": {"type": "int", "feature_latent_dim": 16},
        "user_theoretical_amount_spent": {"type": "int", "feature_latent_dim": 16},
        "user_theoretical_remaining_credit": {"type": "int", "feature_latent_dim": 16},
        "user_distinct_type_booking_cnt": {"type": "int", "feature_latent_dim": 16},
    }
    item_layer_infos = {
        "item_id": {"type": "string", "feature_latent_dim": 128},
        "offer_categoryId": {"type": "string", "feature_latent_dim": 16},
        "offer_subcategoryid": {"type": "string", "feature_latent_dim": 16},
        "item_names": {"type": "text", "feature_latent_dim": 16},
        "item_rayons": {"type": "text", "feature_latent_dim": 16},
        "item_author": {"type": "text", "feature_latent_dim": 16},
        "item_performer": {"type": "text", "feature_latent_dim": 16},
        "item_mean_stock_price": {"type": "int", "feature_latent_dim": 8},
        "item_booking_cnt": {"type": "int", "feature_latent_dim": 8},
        "item_favourite_cnt": {"type": "int", "feature_latent_dim": 8},
    }

    user_columns = list(user_layer_infos.keys())
    item_columns = list(item_layer_infos.keys())

    user_data = train_data[user_columns].drop_duplicates(subset=["user_id"])
    item_data = train_data[item_columns].drop_duplicates(subset=["item_id"])

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

        two_tower_model = TwoTowersModel(
            user_data=user_data,
            user_layer_infos=user_layer_infos,
            item_data=item_data,
            item_layer_infos=item_layer_infos,
            embedding_size=embedding_size,
        )

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
                MLFlowLogging(
                    client_id=client_id,
                    env=ENV_SHORT_NAME,
                    export_path=export_path,
                ),
            ],
        )

        logger.info("Building and saving the TwoTowersMatchModel")

        user_embeddings = two_tower_model.user_model([user_data.values])
        item_embeddings = two_tower_model.item_model([item_data.values])

        match_model = TwoTowersMatchModel(
            user_ids=user_data["user_id"].unique(),
            user_embeddings=user_embeddings,
            item_ids=item_data["item_id"].unique(),
            item_embeddings=item_embeddings,
            embedding_size=embedding_size,
        )
        tf.keras.models.save_model(match_model, export_path + "model")
        mlflow.log_artifacts(export_path + "model", "model")

        # Export the PCA representations of the item embeddings
        os.mkdir(export_path + "pca_plots")
        pca_representations_path = export_path + "pca_plots/"
        save_pca_representation(
            loaded_model=match_model,
            item_data=item_data,
            figures_folder=pca_representations_path,
        )
        mlflow.log_artifacts(export_path + "pca_plots", "pca_plots")

        logger.info("------- TRAINING DONE -------")
        logger.info(mlflow.get_artifact_uri("model"))


if __name__ == "__main__":
    typer.run(train)
