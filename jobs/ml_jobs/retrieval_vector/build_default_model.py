import subprocess

import numpy as np
import tensorflow as tf
import typer
from google.cloud import bigquery
from loguru import logger

from utils import (
    BIGQUERY_CLEAN_DATASET,
    ENV_SHORT_NAME,
    MODELS_RESULTS_TABLE_NAME,
    create_items_table,
    get_item_docs,
    get_items_metadata,
    get_user_docs,
    save_model_type,
)

MODEL_TYPE = {
    "type": "recommendation",
    "default_token": "[UNK]",
}
EMBEDDING_DIMENSION = 64


def get_model_from_mlflow(
    experiment_name: str, run_id: str = None, artifact_uri: str = None
):
    client = bigquery.Client()

    # get artifact_uri from BQ
    if artifact_uri is None or len(artifact_uri) <= 10:
        if run_id is None or len(run_id) <= 2:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        else:
            results_array = (
                client.query(
                    f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}' ORDER BY execution_date DESC LIMIT 1"""
                )
                .to_dataframe()
                .to_dict("records")
            )
        if len(results_array) == 0:
            raise Exception(
                f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}. Failing."
            )
        else:
            artifact_uri = results_array[0]["artifact_uri"]
    return artifact_uri


def download_model(artifact_uri: str, output_path: str) -> None:
    """
    Download model from GCS bucket
    Args:
        artifact_uri (str): GCS bucket path
        output_path (str): local path to save the model
    """
    command = f"gsutil -m cp -r {artifact_uri} {output_path}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        logger.info(line.rstrip().decode("utf-8"))


def prepare_docs() -> None:
    logger.info("Get items metadata...")
    items_df = get_items_metadata()
    logger.info("Items metadata loaded.")

    # download model
    logger.info("Load Two Tower model...")
    tf_reco = tf.keras.models.load_model("./model/")
    logger.info("Two Tower model loaded.")

    # get user and item embeddings
    item_list = tf_reco.item_layer.layers[0].get_vocabulary()
    item_weights = tf_reco.item_layer.layers[1].get_weights()[0].astype(np.float32)
    user_list = tf_reco.user_layer.layers[0].get_vocabulary()
    user_weights = tf_reco.user_layer.layers[1].get_weights()[0].astype(np.float32)
    user_embedding_dict = {x: y for x, y in zip(user_list, user_weights)}
    item_embedding_dict = {x: y for x, y in zip(item_list, item_weights)}

    # build user and item documents
    logger.info("Build user and item documents...")
    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save("./metadata/user.docs")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save("./metadata/item.docs")
    logger.info("User and item documents built.")

    logger.info("Create items lancedb table...")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=EMBEDDING_DIMENSION,
        uri="./metadata/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )
    logger.info("Items lancedb table created.")


def main(
    source_experiment_name: str = typer.Option(
        None,
        help="Source of the experiment",
    ),
    source_artifact_uri: str = typer.Option(
        None,
        help="Source artifact_uri of the model",
    ),
    source_run_id: str = typer.Option(
        None,
        help="Source run_id of the model",
    ),
) -> None:
    if source_artifact_uri is None or len(source_artifact_uri) <= 10:
        logger.info(
            f"Get model from MLFlow experiment {source_experiment_name} with run_id {source_run_id}..."
        )
        source_artifact_uri = get_model_from_mlflow(
            experiment_name=source_experiment_name,
            run_id=source_run_id,
            artifact_uri=source_artifact_uri,
        )
        logger.info(f"Model artifact_uri: {source_artifact_uri}")

    logger.info(f"Download model from {source_artifact_uri} trained model...")
    download_model(artifact_uri=source_artifact_uri, output_path="./")
    logger.info("Model downloaded.")

    logger.info("Building lanceDB table, and user and item docarrays...")
    prepare_docs()
    logger.info("LanceDB table and documents built.")

    save_model_type(model_type=MODEL_TYPE)
    logger.info("Model type saved.")


if __name__ == "__main__":
    typer.run(main)
