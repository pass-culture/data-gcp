import os
import time
from typing import Dict, List, Optional

from loguru import logger

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_LANCE_DB_URI,
    SEMANTIC_BASE_COLUMNS,
    SEMANTIC_DETAIL_COLUMNS,
    SEMANTIC_OUTPUT_METRIC_COLUMNS,
    EmbeddingModelTypes,
)
from app.retrieval.db_bootstrap import ensure_local_semantic_db
from app.retrieval.documents import Document

SEMANTIC_VECTOR_SEARCH_METRIC = "cosine"
# GCS directory (published by the `semantic_search_lancedb` job) holding the
# LanceDB database. When set, it is downloaded to the local `lance_db_uri` at
# startup; when unset the DB is assumed to already be at the local path (tests).
SEMANTIC_LANCE_DB_URI_ENV = "SEMANTIC_LANCE_DB_URI"


class SemanticClient(DefaultClient):
    """Retrieval client for the semantic-embeddings flavor.

    Serves the item semantic embeddings (produced by the `item_embedding` job)
    for two search modes:

    - ``semantic_search``: nearest neighbors of a query *item*'s embedding,
    - ``text_search``: keyword full-text search over ``search_text``.

    The LanceDB table is built + indexed by the standalone ``semantic_search_lancedb``
    job and published to GCS; it is downloaded to local disk at startup rather
    than baked into the docker image. There are no user documents and no ``item.docs``
    store: the query item's vector is fetched straight from the table.
    """

    EMBEDDING_MODEL_TYPE = EmbeddingModelTypes.SEMANTIC

    def __init__(
        self,
        base_columns: List[str] = SEMANTIC_BASE_COLUMNS,
        detail_columns: List[str] = SEMANTIC_DETAIL_COLUMNS,
        output_metric_columns: List[str] = SEMANTIC_OUTPUT_METRIC_COLUMNS,
        lance_db_uri: str = DEFAULT_LANCE_DB_URI,
        vector_search_metric: str = SEMANTIC_VECTOR_SEARCH_METRIC,
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
            lance_db_uri=lance_db_uri,
            vector_search_metric=vector_search_metric,
        )

    def load(self) -> None:
        """Download the LanceDB from GCS (if configured) then connect to it."""
        start_time = time.time()
        gcs_uri = os.environ.get(SEMANTIC_LANCE_DB_URI_ENV)
        if gcs_uri:
            ensure_local_semantic_db(gcs_uri, self.lance_db_uri)
        else:
            logger.info(
                f"{SEMANTIC_LANCE_DB_URI_ENV} unset; using existing DB at "
                f"{self.lance_db_uri}"
            )
        self.table = self.connect_db()
        logger.info(
            f"Connected to semantic database in {time.time() - start_time:.2f} seconds."
        )

    def item_vector(self, item_id: str) -> Optional[Document]:
        """Fetch an item's semantic embedding directly from the LanceDB table.

        Uses the ``item_id`` scalar (BTREE) index for a fast point lookup.
        Args:
            item_id (str): The item identifier.

        Returns:
            Optional[Document]: The item's embedding, or None if not found.
        """
        safe_item_id = str(item_id).replace("'", "''")
        rows = (
            self.table.search()
            .where(f"item_id = '{safe_item_id}'")
            .select(["item_id", "vector"])
            .limit(1)
            .to_list()
        )
        if rows:
            return Document(id=item_id, embedding=rows[0]["vector"])
        return None

    def search_by_text(
        self,
        text: str,
        n: int = 50,
        query_filter: Optional[Dict] = None,
        details: bool = False,
        excluded_items: Optional[List[str]] = None,
        prefilter: bool = False,
    ) -> List[Dict]:
        """Full-text (keyword) search over the ``search_text`` column.

        Args:
            text (str): The free-text query.
            n (int): Maximum number of results to return.
            query_filter (Optional[Dict]): Optional metadata filters (`params`).
            details (bool): Whether to include metadata columns in the results.
            excluded_items (Optional[List[str]]): Item IDs to drop from results.
            prefilter (bool): Apply the metadata filter before (True) or after
                (False) the full-text search.

        Returns:
            List[Dict]: Formatted search results.
        """
        excluded_items = excluded_items or []
        query = self.build_query(query_filter)
        logger.debug(f"FTS query='{text}' filter={query} prefilter={prefilter}")

        search = self.table.search(text, query_type="fts")
        if query:
            search = search.where(query, prefilter=prefilter)

        results = (
            search.select(self.columns(details, re_rank=False))
            .limit(n + len(excluded_items))
            .to_list()
        )

        postprocessed = self.postprocess(
            ranked_items=results, n=n, excluded_items=excluded_items
        )
        return self.format_results(postprocessed, details)
