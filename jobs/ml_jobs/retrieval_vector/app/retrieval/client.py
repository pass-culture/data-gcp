import time
import typing as t
from typing import Dict, List, Optional

import lancedb
from docarray import Document, DocumentArray
from lancedb.table import Table

from app.logger import logger
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    OUTPUT_METRIC_COLUMNS,
)
from app.retrieval.core.filter import Filter
from app.retrieval.utils import load_documents


class DefaultClient:
    """
    DefaultClient class responsible for managing vector-based search and filtering operations
    on items stored in a LanceDB table.

    Attributes:
        table (LanceDB): The LanceDB table containing the vectorized items.
        re_ranker: Optional re-ranking logic for refining search results.
        item_docs (DocumentArray): In-memory storage of item vectors.
    """

    def __init__(
        self,
        base_columns: List[str] = DEFAULT_COLUMNS,
        detail_columns: List[str] = DEFAULT_DETAIL_COLUMNS,
        output_metric_columns: List[str] = OUTPUT_METRIC_COLUMNS,
    ) -> None:
        self.base_columns = base_columns
        self.detail_columns = detail_columns
        self.output_metric_columns = output_metric_columns
        self.table: Optional[Table] = None
        self.re_ranker: Optional[t.Any] = None
        self.item_docs: Optional[Document] = None

    def load(self) -> None:
        """
        Load item documents from the filesystem and connect to the LanceDB database.
        """
        start_time = time.time()
        logger.info("Starting to load item documents and connect to the database...")

        # Load item documents
        start_time = time.time()
        self.item_docs = self.load_item_document()
        logger.info(
            f"Loading item documents took {time.time() - start_time:.2f} seconds."
        )

        # Connect to the database
        start_time = time.time()
        self.table = self.connect_db()
        logger.info(
            f"Connecting to the database took {time.time() - start_time:.2f} seconds."
        )

    def load_item_document(self) -> DocumentArray:
        return load_documents("./metadata/item.docs")

    def connect_db(self) -> Table:
        """
        Establish a connection to the LanceDB vector database.

        Returns:
            Table: The connected LanceDB table instance.
        """
        uri = "./metadata/vector"
        db = lancedb.connect(uri)
        return db.open_table("items")

    def offer_vector(self, item_id: str) -> Optional[Document]:
        """
        Retrieve the vector associated with a specific item ID.

        Args:
            item_id (str): The ID of the item.

        Returns:
            Optional[Document]: The vector document for the given item ID, or None if not found.
        """
        try:
            return self.item_docs[item_id]
        except Exception:
            return None

    def build_query(self, params: Optional[Dict]) -> Optional[str]:
        """
        Build a SQL-like query based on provided filter parameters.

        Args:
            params (Optional[Dict]): Dictionary of filter parameters.

        Returns:
            Optional[str]: SQL-like where clause, or None if no filters are provided.
        """
        if not params:
            return None
        sql = Filter(params).parse_where_clause()
        return sql if sql else None

    def search_by_tops(
        self,
        query_filter: t.Optional[t.Dict] = None,
        n: int = 50,
        details: bool = False,
        prefilter: bool = False,
        vector_column_name: str = "booking_number_desc",
        re_rank: bool = False,
        user_id: t.Optional[str] = None,
    ) -> t.List[t.Dict]:
        """
        Filter the database based on query filters and ranked vector_column_name and optionally rerank results.

        Args:
            query_filter (Optional[Dict]): Optional query filters.
            n (int): Maximum number of results.
            details (bool): Whether to include detailed fields in results.
            vector_column_name (str): Column name to filter on.
            re_rank (bool): Whether to apply re-ranking.
            user_id (str): User ID for re-ranking.

        Returns:
            List[Dict]: A list of filtered results.
        """
        # Approximate the query with a small near-zero value.
        # The items are ranked from 1 to N.
        return self._perform_search(
            vector=Document(embedding=[-1e-2]),
            n=n,
            query_filter=query_filter,
            vector_column_name=vector_column_name,
            details=details,
            prefilter=prefilter,
            re_rank=re_rank,
            user_id=user_id,
        )

    def search_by_vector(
        self,
        vector: Document,
        similarity_metric: str = "dot",
        n: int = 50,
        query_filter: t.Optional[t.Dict] = None,
        details: bool = False,
        item_id: t.Optional[str] = None,
        prefilter: bool = True,
        vector_column_name: str = "vector",
        re_rank: bool = False,
        user_id: t.Optional[str] = None,
    ) -> t.List[t.Dict]:
        """
        Search the vector database for similar items and optionally rerank results.

        Args:
            vector (Document): The vector to search.
            similarity_metric (str): Similarity metric for vector search.
            n (int): Maximum number of results.
            query_filter (Optional[Dict]): Optional query filters.
            details (bool): Whether to include detailed fields in results.
            item_id (Optional[str]): ID of the item to exclude from results.
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
            item_id=item_id,
            prefilter=prefilter,
            re_rank=re_rank,
            user_id=user_id,
        )

    def _perform_search(
        self,
        vector: t.Optional[Document],
        n: int,
        query_filter: t.Optional[t.Dict],
        vector_column_name: str,
        similarity_metric: str = "dot",
        details: bool = False,
        item_id: t.Optional[str] = None,
        user_id: t.Optional[str] = None,
        prefilter: bool = True,
        re_rank: bool = False,
    ) -> List[Dict]:
        """Encapsulate common logic for searching and filtering."""

        query = self.build_query(query_filter)
        logger.debug(f"Build Query {query}")

        results = (
            self.table.search(
                query=vector.embedding,
                vector_column_name=vector_column_name,
                query_type="vector",
            )
            .where(query, prefilter=prefilter)
            .nprobes(20)
            .refine_factor(10)
            .select(columns=self.columns(details, re_rank=re_rank))
            .metric(similarity_metric)
            .limit(n)
        )

        if re_rank and self.re_ranker and user_id:
            results = results.rerank(self.re_ranker, query_string=user_id)

        # ensure results are sorted by distance
        results = results.to_list()
        # results.sort(key=lambda x: x["_distance"])

        return self.format_results(results, details, item_id=item_id)

    def columns(self, details: bool, re_rank: bool) -> List[str]:
        """
        Return the appropriate columns to retrieve from the database.

        Args:
            details (bool): Whether to include detailed metadata.

        Returns:
            List[str]: The list of columns to select.
        """
        return list(
            set(
                self.base_columns
                + (self.detail_columns if details else [])
                + (["vector"] if re_rank else [])
            )
        )

    def format_results(
        self, results: List[Dict], details: bool, item_id: Optional[str] = None
    ) -> List[Dict]:
        """
        Format the raw search results for output.

        Args:
            results (List[Dict]): The raw results from the database.
            details (bool): Whether to include detailed metadata in the output.
            item_id (Optional[str]): Item ID to exclude from the results.

        Returns:
            List[Dict]: A list of formatted results.
        """
        predictions = []
        for idx, row in enumerate(results):
            if item_id and str(row.get("item_id")) == item_id:
                continue

            if not details:
                predictions.append({"idx": idx, "item_id": row["item_id"]})
            else:
                predictions.append(
                    {
                        k: row.get(k)
                        for k in (
                            self.base_columns
                            + self.detail_columns
                            + self.output_metric_columns
                        )
                        if k in row
                    }
                    | {"idx": idx}
                )

        return predictions
