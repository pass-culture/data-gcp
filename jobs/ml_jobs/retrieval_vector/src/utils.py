import json
import subprocess

import pandas as pd
from loguru import logger

from app.retrieval.documents import Document, DocumentArray


def download_model(artifact_uri: str) -> None:
    """
    Download model from GCS bucket
    Args:
        artifact_uri (str): GCS bucket path
    """
    command = f"gsutil -m cp -r {artifact_uri} ."
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}: {e.output}")
        raise


def deploy_container(serving_container, workers):
    """
    Deploy container to Docker registry.

    Args:
        serving_container (str): Container image to deploy
        workers (int): Number of workers for the container

    Raises:
        subprocess.CalledProcessError: If deployment fails
    """
    command = f"sh ./deploy_to_docker_registery.sh {serving_container} {workers}"
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}: {e.output}")
        raise


def save_model_type(model_type: dict, output_dir: str):
    """
    Save model type configuration to JSON file.

    Args:
        model_type (dict): Dictionary containing model type configuration
        output_dir (str): Directory path where the JSON file will be saved
    """
    with open(f"{output_dir}/model_type.json", "w") as file:
        json.dump(model_type, file)


def get_item_docs(item_embedding_dict: dict, items_df: pd.DataFrame) -> DocumentArray:
    """
    Create DocumentArray from item embeddings.

    Builds a DocumentArray containing Documents with item IDs and their
    corresponding embedding vectors.

    Args:
        item_embedding_dict (dict): Mapping of item_id to embedding vectors
        items_df (pd.DataFrame): DataFrame containing item metadata

    Returns:
        DocumentArray: Collection of Documents with item embeddings

    Raises:
        Exception: If no valid documents are created (empty DocumentArray)
    """
    docs = DocumentArray()
    for row in items_df.itertuples():
        item_id = row.item_id
        embedding_id = item_embedding_dict.get(row.item_id)
        if embedding_id is not None:
            docs.append(Document(id=str(item_id), embedding=embedding_id))

    if len(docs) == 0:
        raise Exception("Item Document is empty. Does the model match the query ?")

    return docs


def get_user_docs(user_dict: dict) -> DocumentArray:
    """
    Create DocumentArray from user embeddings.

    Args:
        user_dict (dict): Mapping of user_id to embedding vectors

    Returns:
        DocumentArray: Collection of Documents with user embeddings
    """
    docs = DocumentArray()
    for k, v in user_dict.items():
        docs.append(Document(id=str(k), embedding=v))
    return docs
