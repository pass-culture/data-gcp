from typing import Optional

import numpy as np
import pyarrow as pa
from docarray import Document, DocumentArray
from lancedb.rerankers import Reranker

from app.logging.logger import logger
from app.retrieval.constants import DISTANCE_COLUMN_NAME, USER_DISTANCE_COLUMN_NAME


class UserReranker(Reranker):
    """
    A user-based reranker class that reranks results based on the user's vector.

    Attributes:
        weight (float): The weight to balance between distance and user embedding similarity.
        user_docs (DocumentArray): Document array containing user vectors.
    """

    def __init__(
        self,
        weight: float,
        user_docs: DocumentArray,
        return_score: str = "all",
        vector_column_name: str = "vector",
    ) -> None:
        """
        Initializes the UserReranker with a given weight and user document vectors.

        Args:
            weight (float): Weight to balance between vector distance and user vector similarity.
            user_docs (DocumentArray): Document array containing user embedding vectors.
            return_score (str): Type of score to return (e.g., "all").
            vector_column_name (str): Column field name.
        """
        super().__init__(return_score)
        self.weight = weight
        self.user_docs = user_docs
        self.vector_column_name = vector_column_name

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

    def _user_recommendation(self, vector_results: pa.Table, user_id: str) -> pa.Table:
        """
        Re-ranks vector results based on user-specific vector embeddings.

        Args:
            vector_results (pa.Table): Vector search results as a PyArrow table.
            user_id (str): The user ID whose embedding should be used for reranking.

        Returns:
            pa.Table: The reranked results based on user vector similarity and original vector distance.
        """
        user_doc = self.user_vector(user_id)

        if user_doc:
            scores = np.dot(
                vector_results[self.vector_column_name].to_pylist(), -user_doc.embedding
            )
            updated_distances = self._compute_relevance_score(
                vector_results[DISTANCE_COLUMN_NAME].to_numpy(), np.array(scores)
            )
            # Update score with personalized dot product
            return (
                vector_results.append_column(
                    USER_DISTANCE_COLUMN_NAME, pa.array(scores)
                )
                .drop([DISTANCE_COLUMN_NAME])
                .append_column(DISTANCE_COLUMN_NAME, pa.array(updated_distances))
                .sort_by([(DISTANCE_COLUMN_NAME, "ascending")])
            )
        else:
            logger.debug(f"reranker, user_id not found {user_id}, cannot re_rank")
            return vector_results

    def _compute_relevance_score(
        self, original_distances: np.ndarray, user_distances: np.ndarray
    ) -> np.ndarray:
        """
        Computes the relevance score based on a weighted combination of vector distance and user distance.

        Args:
            original_distances (np.ndarray): The original vector search distances.
            user_distances (np.ndarray): The computed user-specific vector distances.

        Returns:
            np.ndarray: The computed relevance score.
        """
        # Compute the weighted relevance score
        return original_distances * self.weight + user_distances * (1 - self.weight)

    def rerank_vector(self, query: str, vector_results: pa.Table) -> pa.Table:
        """
        Re-rank vector results based on user-specific vector embeddings.

        Args:
            query (str): The user ID, treated as a query.
            vector_results (pa.Table): The vector results to be reranked.

        Returns:
            pa.Table: The reranked vector results.
        """
        return self._user_recommendation(vector_results, user_id=query)

    def rerank_fts(self, query: str, fts_results: pa.Table) -> pa.Table:
        """
        Returns the full-text search (FTS) results without any modifications.

        Args:
            query (str): The search query (currently unused for reranking).
            fts_results (pa.Table): The full-text search results.

        Returns:
            pa.Table: The unmodified FTS results.
        """
        return fts_results

    def rerank_hybrid(
        self, query: str, vector_results: pa.Table, fts_results: pa.Table
    ) -> pa.Table:
        """
        Combines vector results and full-text search results into a single reranked table.

        Args:
            query (str): The user ID, treated as a query.
            vector_results (pa.Table): The vector results to be combined.
            fts_results (pa.Table): The full-text search results to be combined.

        Returns:
            pa.Table: The combined and reranked results from both vector and FTS searches.
        """
        return self.merge_results(vector_results, fts_results)
