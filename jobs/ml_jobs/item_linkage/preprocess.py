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
    reduce_embeddings_and_store_reducer,
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


def preprocess_embedding_and_store_reducer(
    chunk: pd.DataFrame, reducer_path: str, reduction: bool
) -> pd.DataFrame:
    """
    Prepare the table by reading the parquet file from GCS, preprocessing embeddings,
    and merging the embeddings with the dataframe.

    Args:
        chunk (pd.DataFrame): The dataframe to prepare.
        reducer_path (str): The path to store the reducer.
        reduction (bool): Whether to reduce the embeddings.
        linkage_type (str): Type of linkage to perform
    Returns:
        pd.DataFrame: The prepared dataframe with embeddings.
    """
    item_df = chunk
    item_df = item_df[
        item_df["embedding"].apply(lambda vec: not np.all(np.array(vec) == 0))
    ]

    if reduction:
        item_df = item_df.assign(
            vector=reduce_embeddings_and_store_reducer(
                embeddings=preprocess_embeddings_by_chunk(chunk),
                n_dim=MODEL_TYPE["n_dim"],
                reducer_path=reducer_path,
            )
        ).drop(columns=["embedding"])
    else:
        item_df = item_df.assign(
            vector=list(preprocess_embeddings_by_chunk(chunk))
        ).drop(columns=["embedding"])

    embeddings_array = np.array(item_df["vector"].tolist())

    normalized_embeddings = normalize(embeddings_array, norm="l2")

    item_df["vector"] = list(normalized_embeddings)

    return item_df


# Main Typer Command
@app.command()
def main(
    input_path: str = typer.Option(..., help="Path to the input catalog"),
    output_path: str = typer.Option(..., help="Path to save the processed catalog"),
    reduction: str = typer.Option(
        default="true",
        help="Reduce the embeddings",
    ),
    batch_size: int = typer.Option(
        default=PARQUET_BATCH_SIZE,
        help="Batch size for reading the parquet file",
    ),
):
    """
    Process the input catalog in batches,clean and preprocess the tables and optionally reduce the embeddings
    Args:
        input_path (str): Path to the input catalog.
        output_path (str): Path to save the processed catalog.
        reduction (str): Flag ("true"/"false") indicating whether to reduce embeddings.
        batch_size (int): Number of rows to process per chunk when reading the Parquet file.
    """
    reduction = True if reduction == "true" else False
    for i, chunk in enumerate(read_parquet_in_batches_gcs(input_path, batch_size)):
        logger.info(f"Processing chunk {i + 1}...")
        clean_catalog = preprocess_catalog(chunk)
        chunk_ready = preprocess_embedding_and_store_reducer(
            clean_catalog, MODEL_TYPE["reducer_pickle_path"], reduction
        )
        chunk_output_path = f"{output_path}/data-{i + 1}.parquet"
        logger.info(f"Saving processed chunk to {chunk_output_path}...")
        upload_parquet(
            dataframe=chunk_ready,
            gcs_path=chunk_output_path,
        )
        logger.info(f"Chunk {i + 1} processed and saved.")


if __name__ == "__main__":
    app()
