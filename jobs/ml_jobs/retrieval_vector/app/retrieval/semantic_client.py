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
from app.retrieval.documents import Document

SEMANTIC_VECTOR_SEARCH_METRIC = "cosine"


class SemanticClient(DefaultClient):
    """Retrieval client for the semantic-embeddings flavor.

    Serves item semantic embeddings (produced by the `item_embedding` service)
    for two search modes:

    - ``semantic_search``: look the query item's embedding up in the LanceDB table
      and return its nearest neighbors (cosine),
    - ``text_search``: keyword full-text search over ``search_text``.

    Unlike the two-tower / graph clients there are no user documents, and no
    ``item.docs`` store: the query item's vector is fetched straight from the
    table (a 768-dim id→vector JSON dump would be far too large to bake).
    """

    EMBEDDING_MODEL_TYPE = EmbeddingModelTypes.SEMANTIC

    def __init__(
        self,
        base_columns: List[str] = SEMANTIC_BASE_COLUMNS,
        detail_columns: List[str] = SEMANTIC_DETAIL_COLUMNS,
        output_metric_columns: List[str] = SEMANTIC_OUTPUT_METRIC_COLUMNS,
        lance_db_uri: str = DEFAULT_LANCE_DB_URI,
        vector_search_metric: str = SEMANTIC_VECTOR_SEARCH_METRIC,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
            lance_db_uri=lance_db_uri,
            vector_search_metric=vector_search_metric,
            *args,
            **kwargs,
        )

    def load(self) -> None:
        """Connect to the LanceDB table. No item/user documents to load."""
        start_time = time.time()
        self.table = self.connect_db()
        logger.info(
            f"Connected to semantic database in {time.time() - start_time:.2f} seconds."
        )

    def item_vector(self, item_id: str) -> Optional[Document]:
        """Fetch an item's semantic embedding directly from the LanceDB table.

        Uses the ``item_id`` scalar (BTREE) index for a fast point lookup instead
        of relying on a baked ``item.docs`` store.

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
