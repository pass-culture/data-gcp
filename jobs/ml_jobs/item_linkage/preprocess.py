import re
import string
import unicodedata

import numpy as np
import pandas as pd
import typer
from loguru import logger
from sklearn.preprocessing import normalize

from constants import (
    MODEL_TYPE,
    PARQUET_BATCH_SIZE,
    UNKNOWN_PERFORMER,
)
from utils.common import (
    preprocess_embeddings_by_chunk,
    read_parquet_in_batches_gcs,
)
from utils.gcs_utils import upload_parquet

EXTRACT_EDITION_PATTERN = (
    r"\b(?:tome|t|vol|episode)\s*(\d+)\b|\b(?:tome|t|vol|episode)(\d+)\b|(\d+)$"
)
REMOVE_EDITION_PATTERN = (
    r"\b(?:tome|t|vol|episode)\s*\d+\b|\b(?:tome|t|vol|episode)\d+\b|\d+$"
)

app = typer.Typer()


def remove_accents(input_str):
    """
    Removes accents from a given string.
    """
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def preprocess_string(s):
    """
    Preprocesses a string by lowercasing, trimming, removing punctuation, and accents.

    Args:
        s (str): Input string.

    Returns:
        str: Processed string.
    """
    if s is None or s == "":
        return None
    s = s.lower()
    s = s.strip()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(f"[{string.punctuation}]", "", s)
    s = remove_accents(s)
    return s


def preprocess_catalog(catalog: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess the entire catalog DataFrame.

    Args:
        catalog (pd.DataFrame): Catalog DataFrame.

    Returns:
        pd.DataFrame: Processed catalog DataFrame.
    """
    return catalog.assign(
        performer=lambda df: df["performer"]
        .fillna(value=UNKNOWN_PERFORMER)
        .apply(preprocess_string),
        offer_name=lambda df: df["offer_name"].apply(preprocess_string),
        edition=lambda df: df["offer_name"]
        .str.extract(EXTRACT_EDITION_PATTERN, expand=False)[0]
        .replace("nan", "0"),
        oeuvre=lambda df: df["offer_name"]
        .apply(preprocess_string)
        .str.replace(REMOVE_EDITION_PATTERN, "", regex=True),
        offer_description=lambda df: df["offer_description"].apply(preprocess_string),
    )


def preprocess_embeddings(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Drop zero-vector rows, stack the (already truncated) embeddings and L2-normalize
    them into the `vector` column consumed by LanceDB.

    Args:
        chunk (pd.DataFrame): The dataframe to prepare, with an `embedding` column.

    Returns:
        pd.DataFrame: The prepared dataframe with a normalized `vector` column.
    """
    item_df = chunk[
        chunk["embedding"].apply(lambda vec: not np.all(np.array(vec) == 0))
    ]

    item_df = item_df.assign(vector=list(preprocess_embeddings_by_chunk(item_df))).drop(
        columns=["embedding"]
    )

    embeddings_array = np.array(item_df["vector"].tolist())
    if embeddings_array.ndim != 2 or embeddings_array.shape[1] != MODEL_TYPE["n_dim"]:
        raise ValueError(
            f"Expected every embedding to have {MODEL_TYPE['n_dim']} dimensions, "
            f"got an array of shape {embeddings_array.shape}. Check the upstream "
            "truncation in ml_feat__item_embedding_refactor_128."
        )

    normalized_embeddings = normalize(embeddings_array, norm="l2")
    item_df["vector"] = list(normalized_embeddings)

    return item_df


# Main Typer Command
@app.command()
def main(
    input_path: str = typer.Option(..., help="Path to the input catalog"),
    output_path: str = typer.Option(..., help="Path to save the processed catalog"),
    batch_size: int = typer.Option(
        default=PARQUET_BATCH_SIZE,
        help="Batch size for reading the parquet file",
    ),
):
    """
    Process the input catalog in batches: clean the tables and L2-normalize the embeddings.
    Args:
        input_path (str): Path to the input catalog.
        output_path (str): Path to save the processed catalog.
        batch_size (int): Number of rows to process per chunk when reading the Parquet file.
    """
    for i, chunk in enumerate(read_parquet_in_batches_gcs(input_path, batch_size)):
        logger.info(f"Processing chunk {i + 1}...")
        clean_catalog = preprocess_catalog(chunk)
        chunk_ready = preprocess_embeddings(clean_catalog)
        chunk_output_path = f"{output_path}/data-{i + 1}.parquet"
        logger.info(f"Saving processed chunk to {chunk_output_path}...")
        upload_parquet(
            dataframe=chunk_ready,
            gcs_path=chunk_output_path,
        )
        logger.info(f"Chunk {i + 1} processed and saved.")


if __name__ == "__main__":
    app()
