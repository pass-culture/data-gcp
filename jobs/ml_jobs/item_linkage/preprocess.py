import re
import string
import unicodedata

import numpy as np
import pandas as pd
import typer
from loguru import logger
from sklearn.preprocessing import normalize

# Constants
from constants import (
    MODEL_TYPE,
    PARQUET_BATCH_SIZE,
    UNKNOWN_PERFORMER,
    extract_pattern,
    remove_pattern,
)
from utils.common import (
    preprocess_embeddings_by_chunk,
    read_parquet_in_batches_gcs,
    reduce_embeddings_and_store_reducer,
)
from utils.gcs_utils import upload_parquet

# Typer app instance
app = typer.Typer()


# Helper Functions
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
    if s is None:
        return s
    s = s.lower()
    s = s.strip()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(f"[{string.punctuation}]", "", s)
    return remove_accents(s)


# Preprocessing Function
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
        .replace("", None)
        .str.lower(),
        edition=lambda df: df["offer_name"]
        .str.extract(extract_pattern, expand=False)
        .astype(str)
        .fillna(value="1"),
        # Ne pas ecraser la colonne offer_name et passer vers une colonne oeuvre
        offer_name=lambda df: df["offer_name"]
        .str.replace(remove_pattern, "", regex=True)
        .str.strip()
        .replace("", None)
        .apply(preprocess_string),
        offer_description=lambda df: df["offer_description"]
        .replace("", None)
        .str.lower(),
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
    # Remove all zeroes embeddings before reducing
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

    # Convert embeddings to a NumPy array
    embeddings_array = np.array(item_df["vector"].tolist())

    # Normalize the embeddings
    normalized_embeddings = normalize(embeddings_array, norm="l2")

    # Update the DataFrame with normalized embeddings
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
    Main function to preprocess catalog data.
    """
    reduction = True if reduction == "true" else False
    for i, chunk in enumerate(read_parquet_in_batches_gcs(input_path, batch_size)):
        logger.info(f"Processing chunk {i + 1}...")
        preprocessed_chunk = preprocess_catalog(chunk)
        clean_chunk = preprocess_embedding_and_store_reducer(
            preprocessed_chunk, MODEL_TYPE["reducer_pickle_path"], reduction
        )
        chunk_output_path = f"{output_path}/data-{i + 1}.parquet"
        logger.info(f"Saving processed chunk to {chunk_output_path}...")
        upload_parquet(
            dataframe=clean_chunk,
            gcs_path=chunk_output_path,
        )
        logger.info(f"Chunk {i + 1} processed and saved.")


if __name__ == "__main__":
    app()
