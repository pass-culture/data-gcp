import time
from typing import Dict, List, Optional

import lancedb
import numpy as np
from docarray import Document, DocumentArray
from lancedb.rerankers import Reranker
from lancedb.table import Table

from app.logging.logger import logger
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    DEFAULT_ITEM_DOCS_PATH,
    DEFAULT_LANCE_DB_URI,
    DEFAULT_USER_DOCS_PATH,
    OUTPUT_METRIC_COLUMNS,
    SIMILARITY_ITEM_ITEM_COLUMN_NAME,
    SIMILARITY_USER_ITEM_COLUMN_NAME,
)
from app.retrieval.core.filter import Filter
from app.retrieval.utils import load_documents


class DefaultClient:
    """
    DefaultClient class responsible for managing vector-based search and filtering operations
    on items stored in a LanceDB table.
    """

    def __init__(
        self,
        base_columns: List[str] = DEFAULT_COLUMNS,
        detail_columns: List[str] = DEFAULT_DETAIL_COLUMNS,
        output_metric_columns: List[str] = OUTPUT_METRIC_COLUMNS,
        item_docs_path: str = DEFAULT_ITEM_DOCS_PATH,
        user_docs_path: str = DEFAULT_USER_DOCS_PATH,
        lance_db_uri: str = DEFAULT_LANCE_DB_URI,
    ) -> None:
        self.base_columns = base_columns
        self.detail_columns = detail_columns
        self.output_metric_columns = output_metric_columns
        self.item_docs_path = item_docs_path
        self.user_docs_path = user_docs_path
        self.lance_db_uri = lance_db_uri
        self.table: Optional[Table] = None
        self.re_ranker: Optional[Reranker] = None
        self.item_docs: Optional[Document] = None

    def load(self) -> None:
        """
        Load item documents from the filesystem and connect to the LanceDB database.
        """
        start_time = time.time()
        logger.info("Starting to load item documents and connect to the database...")

        # Load item and user documents
        start_time = time.time()
        self.item_docs = self.load_item_document()
        self.user_docs = self.load_user_document()
        logger.info(
            f"Loading item and user documents took {time.time() - start_time:.2f} seconds."
        )

        # Connect to the database
        start_time = time.time()
        self.table = self.connect_db()
        logger.info(
            f"Connecting to the database took {time.time() - start_time:.2f} seconds."
        )

    def load_item_document(self) -> DocumentArray:
        return load_documents(self.item_docs_path)

    def load_user_document(self) -> DocumentArray:
        return load_documents(self.user_docs_path)

    def user_vector(self, user_id: str) -> Optional[Document]:
        """
        Retrieves the user vector from the document array based on the given user ID.

        Args:
            user_id (str): The ID of the user.

        Returns:
            Optional[Document]: The user's document embedding, or None if not found.
        """
        if user_id in self.user_docs:
            return self.user_docs[user_id]
        return None

    def item_vector(self, item_id: str) -> Optional[Document]:
        """
        Retrieve the vector associated with a specific item ID.

        Args:
            item_id (str): The ID of the item.

        Returns:
            Optional[Document]: The vector document for the given item ID, or None if not found.
        """
        if item_id in self.item_docs:
            return self.item_docs[item_id]
        return None

    def connect_db(self) -> Table:
        """
        Establish a connection to the LanceDB vector database.

        Returns:
            Table: The connected LanceDB table instance.
        """
        db = lancedb.connect(self.lance_db_uri)
        return db.open_table("items")

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
        return Filter(params).parse_where_clause()

    def search_by_tops(
        self,
        query_filter: Optional[Dict] = None,
        n: int = 50,
        details: bool = False,
        prefilter: bool = False,
        vector_column_name: str = "booking_number_desc",
        similarity_metric: str = "dot",
        re_rank: bool = False,
        user_id: Optional[str] = None,
        item_ids: List[str] = [],
    ) -> List[Dict]:
        """
        Perform a search query that filters and ranks items based on a specified vector column,
        with an option for re-ranking the results.

        The function leverages a constant vector `DEFAULT_APPROX_TOP_VECTOR` to approximate rankings
        by the `vector_column_name` (default is "booking_number_desc"), typically representing
        a "most booked" status. This approach uses a near-zero value vector with dot product similarity
        to reverse rank items.

        ### Logic:
        - **1D Vector Representation**: During preprocessing, each item is assigned a value between 1 (most booked)
        and N (least booked) in a 1-dimensional vector space.
        - **Dot Product Similarity**: A small negative value (-0.00001) is used in the dot product calculation,
        giving priority to items closer to 0 (most booked).
        - **Approximation**: A small vector value (-0.0001) simplifies the computation, ensuring that items
        with higher booking counts are returned first.

        ### Example:
        Using `DEFAULT_APPROX_TOP_VECTOR`, items are ranked based on their booking count:

        - Item A – 1000 bookings (Rank 1)
        - Item B – 850 bookings (Rank 2)
        - Item C – 500 bookings (Rank 3)
        - ...

        ### Args:
            query_filter (Optional[Dict]): Optional filters to narrow the search (e.g., {"location": "Paris"}).
            n (int): Maximum number of results to return. Default is 50.
            details (bool): Whether to include additional fields in the result. Default is False.
            vector_column_name (str): Column used for ranking. Default is "booking_number_desc".
            similarity_metric (str): Metric for ranking, typically "dot" (dot product similarity).
            re_rank (bool): Whether to apply a secondary re-ranking process. Default is False.
            user_id (Optional[str]): User ID for personalized ranking. Default is None. If provided, will also compute
                similarity scores for the user.
            item_ids (List[str]): List of item IDs to include in the search. Default is empty list. If provided, will also compute
                similarity scores for the items.

        ### Returns:
            List[Dict]: A list of dictionaries, where each represents a ranked item.
        """

        DEFAULT_APPROX_TOP_VECTOR = Document(embedding=[-0.0001])

        return self._perform_search(
            vector=DEFAULT_APPROX_TOP_VECTOR,
            n=n,
            query_filter=query_filter,
            vector_column_name=vector_column_name,
            similarity_metric=similarity_metric,
            details=details,
            prefilter=prefilter,
            re_rank=re_rank,
            user_id=user_id,
            item_ids=item_ids,
        )

    def search_by_vector(
        self,
        vector: Document,
        similarity_metric: str = "dot",
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

    def _perform_search(
        self,
        vector: Optional[Document],
        n: int,
        query_filter: Optional[Dict],
        vector_column_name: str,
        similarity_metric: str = "dot",
        details: bool = False,
        excluded_items: List[str] = [],
        user_id: Optional[str] = None,
        item_ids: List[str] = [],
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

        postprocessed_results = self._add_item_dot_similarities(
            ranked_items=results.to_list(), user_id=user_id, input_item_ids=item_ids
        )
        return self.format_results(
            postprocessed_results, details, excluded_items=excluded_items
        )

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
        self, results: List[Dict], details: bool, excluded_items: List[str] = []
    ) -> List[Dict]:
        """
        Format the raw search results for output.

        Args:
            results (List[Dict]): The raw results from the database.
            details (bool): Whether to include detailed metadata in the output.
            excluded_items (List[str]): Item ID to exclude from the results.

        Returns:
            List[Dict]: A list of formatted results.
        """
        predictions = []
        for idx, row in enumerate(results):
            if str(row.get("item_id")) in excluded_items:
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

    def _add_item_dot_similarities(
        self,
        ranked_items: List[Dict],
        user_id: Optional[str],
        input_item_ids: List[str],
    ) -> List[Dict]:
        """
        Adds dot product similarity scores between a user's vector and item vectors.

        This method calculates the dot product similarity between the embedding vector
        of the specified user and the embedding vector of each item in the `ranked_items` list.
        The similarity score is added to each item's dictionary under the
        key specified by `SIMILARITY_COLUMN_NAME`.

        If `user_id` is not provided, or if the user vector or an item vector
        cannot be found, the similarity score will be `None`.

        Args:
            user_id (str): The unique identifier for the user. If None,
                   similarity scores will be set to None.
            item_ids (List[str]): A list of item IDs to retrieve their vectors.
            ranked_items (List[Dict]): A list of dictionaries, where each dictionary
                           represents an item and should contain an "item_id"
                           key to retrieve its vector.

        Returns:
            List[Dict]: The input list of `ranked_items`, with each item dictionary
                updated to include a `SIMILARITY_COLUMN_NAME` key holding
                the dot product similarity score (float) or None.
        """
        user_vector = self.user_vector(user_id)
        input_item_vectors = {
            item_id: self.item_vector(item_id) for item_id in input_item_ids
        }
        ranked_item_vectors = {
            item["item_id"]: self.item_vector(item["item_id"]) for item in ranked_items
        }

        if len(input_item_vectors) > 0:
            for item in ranked_items:
                item_id = item.get("item_id")
                item_vector = ranked_item_vectors[item_id]
                item[SIMILARITY_USER_ITEM_COLUMN_NAME] = (
                    float(np.dot(user_vector.embedding, item_vector.embedding))
                    if user_vector
                    else None
                )

                item[SIMILARITY_ITEM_ITEM_COLUMN_NAME] = {}
                for input_item_id in input_item_ids:
                    input_item_vector = input_item_vectors.get(input_item_id)
                    item[SIMILARITY_ITEM_ITEM_COLUMN_NAME][input_item_id] = (
                        float(
                            np.dot(
                                item_vector.embedding,
                                input_item_vector.embedding,
                            )
                        )
                        if item_vector and input_item_vector
                        else None
                    )
        else:
            for item in ranked_items:
                item_id = item.get("item_id")
                item[SIMILARITY_ITEM_ITEM_COLUMN_NAME] = {}
                item[SIMILARITY_USER_ITEM_COLUMN_NAME] = (
                    float(
                        np.dot(
                            user_vector.embedding,
                            ranked_item_vectors[item_id].embedding,
                        )
                    )
                    if user_vector
                    else None
                )
        return ranked_items
