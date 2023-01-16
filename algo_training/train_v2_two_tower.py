import os

import mlflow
import typer

import tensorflow as tf
from loguru import logger
import pandas as pd

from models.two_towers.match_model import TwoTowersMatchModel
from models.two_towers.models import TwoTowersModel
from models.two_towers.layers import (
    StringEmbeddingLayer,
    IntegerEmbeddingLayer,
    TextEmbeddingLayer,
)
from models.two_towers.utils import build_dict_dataset
from models.v2.utils import (
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
TRAINING_STEPS = 100
VALIDATION_STEPS = 20
LEARNING_RATE = 0.1
VERBOSE = 2

user_embedding_layers = {
    "user_id": StringEmbeddingLayer(embedding_size=64),
    "user_age": IntegerEmbeddingLayer(embedding_size=8),
    "user_postal_code": StringEmbeddingLayer(embedding_size=8),
    "user_activity": StringEmbeddingLayer(embedding_size=8),
    "user_booking_cnt": IntegerEmbeddingLayer(embedding_size=8),
    "user_theoretical_amount_spent": IntegerEmbeddingLayer(embedding_size=8),
    "user_theoretical_remaining_credit": IntegerEmbeddingLayer(embedding_size=8),
    "user_distinct_type_booking_cnt": IntegerEmbeddingLayer(embedding_size=8),
}
item_embedding_layers = {
    "item_id": StringEmbeddingLayer(embedding_size=64),
    "offer_categoryId": StringEmbeddingLayer(embedding_size=16),
    "offer_subcategoryid": StringEmbeddingLayer(embedding_size=16),
    "item_names": TextEmbeddingLayer(embedding_size=8),
    "item_rayons": TextEmbeddingLayer(embedding_size=8),
    "item_author": TextEmbeddingLayer(embedding_size=8),
    "item_performer": TextEmbeddingLayer(embedding_size=8),
    "item_mean_stock_price": IntegerEmbeddingLayer(embedding_size=8),
    "item_booking_cnt": IntegerEmbeddingLayer(embedding_size=8),
    "item_favourite_cnt": IntegerEmbeddingLayer(embedding_size=8),
}


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

    logger.info("Loading & processing datasets")

    # Load BigQuery data
    train_data = pd.read_csv(
        f"{STORAGE_PATH}/positive_data_train.csv",
    ).astype(str)
    validation_data = pd.read_csv(
        f"{STORAGE_PATH}/positive_data_eval.csv",
    ).astype(str)

    user_columns = list(user_embedding_layers.keys())
    item_columns = list(item_embedding_layers.keys())
    user_data = train_data[user_columns].drop_duplicates(subset=["user_id"])
    item_data = train_data[item_columns].drop_duplicates(subset=["item_id"])

    # Build tf datasets
    train_dataset = build_dict_dataset(train_data, batch_size=batch_size, seed=seed)
    validation_dataset = build_dict_dataset(
        validation_data, batch_size=batch_size, seed=seed
    )

    user_dataset = build_dict_dataset(
        user_data,
        batch_size=batch_size,
        seed=seed,
    )
    item_dataset = build_dict_dataset(
        item_data,
        batch_size=batch_size,
        seed=seed,
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
                "user_count": len(user_data),
                "item_count": len(item_data),
            }
        )

        two_tower_model = TwoTowersModel(
            data=train_data,
            user_embedding_layers=user_embedding_layers,
            item_embedding_layers=item_embedding_layers,
            items_dataset=item_dataset,
            embedding_size=embedding_size,
        )

        two_tower_model.compile(
            loss=tf.keras.optimizers.Adagrad(learning_rate=LEARNING_RATE),
            optimizer="adam",
        )

        two_tower_model.fit(
            train_dataset,
            epochs=N_EPOCHS,
            steps_per_epoch=TRAINING_STEPS,
            validation_data=validation_dataset,
            validation_steps=VALIDATION_STEPS,
            verbose=VERBOSE,
            callbacks=[
                tf.keras.callbacks.ReduceLROnPlateau(
                    monitor="val_factorized_top_k/top_100_categorical_accuracy",
                    factor=0.1,
                    patience=2,
                    min_delta=0,
                    verbose=1,
                ),
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_factorized_top_k/top_100_categorical_accuracy",
                    patience=3,
                    min_delta=0,
                    verbose=1,
                ),
                MLFlowLogging(
                    client_id=client_id,
                    env=ENV_SHORT_NAME,
                    export_path=export_path,
                ),
            ],
        )

        logger.info("Building and saving the TwoTowersMatchModel")

        user_embeddings = two_tower_model.user_model.predict(user_dataset)
        item_embeddings = two_tower_model.item_model.predict(item_dataset)

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
        os.makedirs(export_path + "pca_plots", exist_ok=True)
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
