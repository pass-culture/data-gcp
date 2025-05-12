import subprocess

import numpy as np
import tensorflow as tf
import typer
from google.cloud import bigquery

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


def download_model(artifact_uri):
    command = f"gsutil -m cp -r {artifact_uri} ./"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def prepare_docs(normalize: bool = False) -> None:
    print("Get items...")
    items_df = get_items_metadata()
    # download model
    print("Get model...")
    tf_reco = tf.keras.models.load_model("./model/")
    item_list = tf_reco.item_layer.layers[0].get_vocabulary()
    item_weights = tf_reco.item_layer.layers[1].get_weights()[0].astype(np.float32)
    user_list = tf_reco.user_layer.layers[0].get_vocabulary()
    user_weights = tf_reco.user_layer.layers[1].get_weights()[0].astype(np.float32)
    # convert
    if normalize:
        user_embedding_dict = {
            x: y / np.linalg.norm(y) for x, y in zip(user_list, user_weights)
        }
        item_embedding_dict = {
            x: y / np.linalg.norm(y) for x, y in zip(item_list, item_weights)
        }
    else:
        user_embedding_dict = {x: y for x, y in zip(user_list, user_weights)}
        item_embedding_dict = {x: y for x, y in zip(item_list, item_weights)}

    user_docs = get_user_docs(user_embedding_dict)
    user_docs.save("./metadata/user.docs")
    item_docs = get_item_docs(item_embedding_dict, items_df)
    item_docs.save("./metadata/item.docs")
    create_items_table(
        item_embedding_dict,
        items_df,
        emb_size=EMBEDDING_DIMENSION,
        uri="./metadata/vector",
        create_index=True if ENV_SHORT_NAME == "prod" else False,
    )


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
    normalize: bool = typer.Option(
        False,
        help="Normalize embeddings",
    ),
) -> None:
    if source_artifact_uri is None or len(source_artifact_uri) <= 10:
        source_artifact_uri = get_model_from_mlflow(
            experiment_name=source_experiment_name,
            run_id=source_run_id,
            artifact_uri=source_artifact_uri,
        )
    print(f"Get from {source_artifact_uri} trained model")
    print("Download...")
    download_model(source_artifact_uri)

    print("Build vector database")
    prepare_docs(normalize=normalize)
    save_model_type(model_type=MODEL_TYPE)


if __name__ == "__main__":
    typer.run(main)
