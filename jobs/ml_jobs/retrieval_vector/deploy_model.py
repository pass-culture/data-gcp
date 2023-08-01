import pandas as pd
from datetime import datetime
import subprocess
import typer
from utils import (
    BIGQUERY_CLEAN_DATASET,
    MODELS_RESULTS_TABLE_NAME,
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    deploy_container,
    get_items_metadata,
    get_user_docs,
    get_item_docs,
    save_experiment,
    save_model_type,
)
import tensorflow as tf
import numpy as np


MODEL_TYPE = {
    "n_dim": 64,
    "metric": "cosine",
    "type": "recommendation",
    "default_token": "[UNK]",
}


def get_model_from_mlflow(
    experiment_name: str, run_id: str = None, artifact_uri: str = None
):
    # get artifact_uri from BQ
    if artifact_uri is None or len(artifact_uri) <= 10:
        if run_id is None or len(run_id) <= 2:
            results_array = pd.read_gbq(
                f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
            ).to_dict("records")
        else:
            results_array = pd.read_gbq(
                f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}' ORDER BY execution_date DESC LIMIT 1"""
            ).to_dict("records")
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
    # export weights to npy format


def prepare_docs(model_type):
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
    user_embedding_dict = {x: y for x, y in zip(user_list, user_weights)}
    item_embedding_dict = {x: y for x, y in zip(item_list, item_weights)}

    user_docs = get_user_docs(user_embedding_dict, MODEL_TYPE["metric"])
    user_docs.save(
        "./metadata/user.docs"
    )  # .save_binary("./metadata/user.bin", protocol='protobuf', compress='lz4')
    item_docs = get_item_docs(
        item_embedding_dict, items_df, model_type["n_dim"], model_type["metric"]
    )
    item_docs.save("./metadata/item.docs")


def main(
    experiment_name: str = typer.Option(
        None,
        help="Name of the experiment",
    ),
    model_name: str = typer.Option(
        None,
        help="Name of the model",
    ),
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

    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = (
        f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    )
    if source_artifact_uri is None or len(source_artifact_uri) <= 10:
        source_artifact_uri = get_model_from_mlflow(
            experiment_name=source_experiment_name,
            run_id=source_run_id,
            artifact_uri=source_artifact_uri,
        )
    print(f"Get from {source_artifact_uri} trained model")
    print(f"Download...")
    download_model(source_artifact_uri)
    prepare_docs(model_type=MODEL_TYPE)
    print("Deploy...")
    save_model_type(model_type=MODEL_TYPE)
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
