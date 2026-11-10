"""The embeddings query for one corpus (spec section 5).

Filtered on the corpus's GTL ids (at most a few hundred values) rather than
an item id list, which could exceed BigQuery's request size limit at full
scale. ``array_slice(x, 0, dim - 1)`` is inclusive of the end index, so
``dim=256`` keeps the first 256 values.
"""

from google.cloud import bigquery

from src.common.constants import (
    BIGQUERY_ML_INPUT_DATASET,
    BIGQUERY_ML_SEMANTIC_EMBEDDING_DATASET,
    GCP_PROJECT_ID,
)

EMBEDDINGS_QUERY = f"""
select
    item_id,
    array_slice(books_metadata_embedding, 0, @dim - 1) as embedding
from `{GCP_PROJECT_ID}.{BIGQUERY_ML_SEMANTIC_EMBEDDING_DATASET}.books_metadata`
where item_id in (
    select item_id
    from `{GCP_PROJECT_ID}.{BIGQUERY_ML_INPUT_DATASET}.item_metadata`
    where gtl_type = 'BOOK' and titelive_gtl_id in unnest(@gtl_ids)
)
""".strip()


def embeddings_query_parameters(
    dim: int, gtl_ids: list[str]
) -> list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]:
    return [
        bigquery.ScalarQueryParameter("dim", "INT64", dim),
        bigquery.ArrayQueryParameter("gtl_ids", "STRING", gtl_ids),
    ]
