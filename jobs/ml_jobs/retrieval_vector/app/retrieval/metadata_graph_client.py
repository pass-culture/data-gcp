import time
from typing import Dict, List, Optional

from docarray import Document
from loguru import logger

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    DEFAULT_ITEM_DOCS_PATH,
    DEFAULT_LANCE_DB_URI,
    OUTPUT_METRIC_COLUMNS,
    EmbeddingModelTypes,
)


class MetadataGraphClient(DefaultClient):
    EMBEDDING_MODEL_TYPE = EmbeddingModelTypes.METADATA_GRAPH

    def __init__(
        self,
        default_token: str,
        base_columns: List[str] = DEFAULT_COLUMNS,
        detail_columns: List[str] = DEFAULT_DETAIL_COLUMNS,
        output_metric_columns: List[str] = OUTPUT_METRIC_COLUMNS,
        item_docs_path: str = DEFAULT_ITEM_DOCS_PATH,
        lance_db_uri: str = DEFAULT_LANCE_DB_URI,
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
            item_docs_path=item_docs_path,
            lance_db_uri=lance_db_uri,
        )
        self.default_token = default_token

    def load(self) -> None:
        """
        Load only item documents and connect to the database.
        Overrides the load method in the DefaultClient class, not to load the user documents.
        """

        # Load only item documents
        start_time = time.time()
        self.item_docs = self.load_item_document()
        logger.info(
            f"Item documents loaded for Metadata Graph Retrieval in {time.time() - start_time:.2f} seconds."
        )

        # Connect to the database
        start_time = time.time()
        self.table = self.connect_db()
        logger.info(f"Connected to database in {time.time() - start_time:.2f} seconds.")

    def search_by_vector(
        self,
        vector: Document,
        similarity_metric: str = "cosine",
        n: int = 50,
        query_filter: Optional[Dict] = None,
        details: bool = False,
        excluded_items: List[str] = [],
        prefilter: bool = True,
        vector_column_name: str = "vector",
        re_rank: bool = False,
        user_id: Optional[str] = None,
        item_ids: List[str] = [],
    ) -> List[Dict]:
        """
        Search the vector database for similar items and optionally rerank results.

        Args:
            vector (Document): The vector to search.
            similarity_metric (str): Similarity metric for vector search.
            n (int): Maximum number of results.
            query_filter (Optional[Dict]): Optional query filters.
            details (bool): Whether to include detailed fields in results.
            excluded_items (List[str]): ID of the item to exclude from results.
            prefilter (bool): Whether to apply pre-filtering.
            vector_column_name (str): Column name to search on.
            re_rank (bool): Whether to apply re-ranking.
            user_id (str): User ID for re-ranking.

        Returns:
            List[Dict]: A list of search results.
        """
        return self._perform_search(
            vector=vector,
            n=n,
            query_filter=query_filter,
            vector_column_name=vector_column_name,
            similarity_metric=similarity_metric,
            details=details,
            excluded_items=excluded_items,
            prefilter=prefilter,
            re_rank=re_rank,
            user_id=user_id,
            item_ids=item_ids,
        )
