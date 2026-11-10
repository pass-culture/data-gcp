"""Fetching and validating corpus embeddings (spec section 7.2, step 1).

Embeddings are read as a prefix of the 768-d book embedding ("Matryoshka"
training means a prefix carries most of the meaning), then L2-renormalized
-- the prefix's own length drifts from 1, and mixing that leftover length
into distances would bias UMAP/HDBSCAN.
"""

import numpy as np
import polars as pl
from loguru import logger

from src.common.bigquery import read_dataframe
from src.reduction.queries import EMBEDDINGS_QUERY, embeddings_query_parameters

NORM_TOLERANCE = 1e-3


def fetch_embeddings(dim: int, gtl_ids: list[str]) -> pl.DataFrame:
    """Query, truncate to ``dim``, and L2-renormalize one corpus's embeddings.

    Zero-norm rows (pathological, e.g. an all-zero embedding) are left
    unchanged and flagged in a ``zero_norm`` column rather than renormalized
    (dividing by a zero norm would produce NaNs).
    """
    raw = read_dataframe(EMBEDDINGS_QUERY, embeddings_query_parameters(dim, gtl_ids))
    vectors = np.asarray(raw["embedding"].to_list(), dtype=np.float32)
    norms = np.linalg.norm(vectors, axis=1)
    zero_norm = norms == 0
    safe_norms = np.where(zero_norm, 1.0, norms)
    normalized = (vectors / safe_norms[:, None]).astype(np.float32)

    if zero_norm.any():
        logger.warning(f"{zero_norm.sum()} item(s) have a zero-norm embedding")

    return pl.DataFrame(
        {
            "item_id": raw["item_id"],
            "embedding": normalized.tolist(),
            "zero_norm": zero_norm.tolist(),
        }
    )


def join_with_documents(
    embeddings: pl.DataFrame, documents: pl.DataFrame
) -> pl.DataFrame:
    """Inner-join embeddings with the corpus's non-empty documents (section
    7.2 step 3), preserving ``documents``' row order, and logging + dropping
    items without a matching embedding rather than failing the whole corpus.
    """
    joined = documents.join(
        embeddings, on="item_id", how="inner", maintain_order="left"
    )
    missing = documents.height - joined.height
    if missing:
        logger.warning(f"{missing} document(s) have no matching embedding, skipped")
    return joined


def validate_embeddings(data: pl.DataFrame, expected_item_ids: list[str]) -> None:
    """Strict alignment/invariant check (spec section 9's "Reduction" row):
    float32 dtype, unit-norm rows (within ``NORM_TOLERANCE``, zero-norm rows
    excepted), and ``data``'s item order/length exactly matching
    ``expected_item_ids``. Raises ``ValueError`` on any violation.
    """
    if data["item_id"].to_list() != expected_item_ids:
        raise ValueError(
            f"Embeddings item order/length ({data.height}) does not match "
            f"the expected documents ({len(expected_item_ids)})"
        )

    vectors = np.asarray(data["embedding"].to_list(), dtype=np.float32)
    if vectors.dtype != np.float32:
        raise ValueError(f"Expected float32 embeddings, got {vectors.dtype}")

    norms = np.linalg.norm(vectors, axis=1)
    non_zero = ~data["zero_norm"].to_numpy()
    out_of_tolerance = non_zero & (np.abs(norms - 1.0) > NORM_TOLERANCE)
    if out_of_tolerance.any():
        bad_item_ids = data["item_id"].to_numpy()[out_of_tolerance][:5].tolist()
        raise ValueError(
            f"{out_of_tolerance.sum()} embedding(s) are not unit-norm within "
            f"{NORM_TOLERANCE}: {bad_item_ids}..."
        )
