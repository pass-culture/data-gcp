"""
Load the precomputed book embeddings (google/embeddinggemma-300m, see
jobs/ml_jobs/item_embedding/configs/books_metadata.yaml) for a set of items.

The query keeps the first 128 of the 768 dimensions: EmbeddingGemma is
trained with Matryoshka Representation Learning, so a prefix of the vector is
itself a valid (coarser) embedding -- once re-normalized to unit length.

The embedding table is neither partitioned nor clustered, so every query
scans it fully (~17 GB) whatever the item filter: fetched embeddings are
cached per table, and only items missing from the cache are queried.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from src.config import EMBEDDING_DATASET, GCP_PROJECT_ID

QUERIES_DIR = Path(__file__).parent / "queries"
EMBEDDINGS_QUERY = (QUERIES_DIR / "books_semantic_embeddings.sql").read_text()
EMBEDDING_COL = "embedding_128"


def embeddings_cache_path(cache_dir: str, project: str, dataset: str) -> Path:
    return Path(cache_dir) / f"embeddings__{project}__{dataset}.parquet"


def _query_embeddings(
    item_ids: list[str], project: str, embedding_dataset: str
) -> pd.DataFrame:
    from google.cloud import bigquery

    logger.info(
        f"Querying embeddings of {len(item_ids)} items from BigQuery "
        f"project={project}, dataset={embedding_dataset} (full table scan)"
    )
    query = EMBEDDINGS_QUERY.format(
        project=project, embedding_dataset=embedding_dataset
    )
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("item_ids", "STRING", item_ids)]
    )
    client = bigquery.Client(project=project)
    return client.query(query, job_config=config).to_dataframe()


def load_embeddings(
    item_ids: list[str],
    cache_dir: str | None,
    project: str = GCP_PROJECT_ID,
    embedding_dataset: str = EMBEDDING_DATASET,
) -> pd.DataFrame:
    """item_id + EMBEDDING_COL for every requested item that has an
    embedding (items without one are logged and left out). Items found to
    have no embedding are cached too (with a null embedding), so they don't
    trigger a new full-table scan on every run."""
    cache = (
        embeddings_cache_path(cache_dir, project, embedding_dataset)
        if cache_dir
        else None
    )
    cached = (
        pd.read_parquet(cache)
        if cache and cache.exists()
        else pd.DataFrame({"item_id": pd.Series(dtype=str), EMBEDDING_COL: []})
    )
    missing = sorted(set(item_ids) - set(cached["item_id"]))
    if missing:
        fetched = _query_embeddings(missing, project, embedding_dataset)
        absent = pd.DataFrame(
            {"item_id": sorted(set(missing) - set(fetched["item_id"]))}
        )
        cached = pd.concat([cached, fetched, absent], ignore_index=True)
        if cache:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cached.to_parquet(cache, index=False)
            logger.info(f"Cached {len(cached)} embeddings to {cache}")
    else:
        logger.info(f"All {len(item_ids)} embeddings found in cache {cache}")
    found = cached[cached["item_id"].isin(item_ids) & cached[EMBEDDING_COL].notna()]
    if len(found) < len(set(item_ids)):
        logger.warning(
            f"{len(set(item_ids)) - len(found)}/{len(set(item_ids))} items have "
            f"no embedding in {embedding_dataset}.books_metadata -- left out"
        )
    return found.reset_index(drop=True)


def to_matrix(embeddings: pd.Series) -> np.ndarray:
    """Stack the per-item vectors and re-normalize them to unit length
    (required after Matryoshka truncation)."""
    matrix = np.vstack(embeddings.to_numpy()).astype(np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.where(norms == 0, 1, norms)
