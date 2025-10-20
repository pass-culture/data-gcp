from typing import Dict, List, Optional

import numpy as np

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    DEFAULT_ITEM_DOCS_PATH,
    DEFAULT_LANCE_DB_URI,
    DEFAULT_USER_DOCS_PATH,
    OUTPUT_METRIC_COLUMNS,
    SIMILARITY_ITEM_ITEM_COLUMN_NAME,
    SIMILARITY_USER_ITEM_COLUMN_NAME,
    EmbeddingModelTypes,
)
from app.retrieval.core.reranker import UserReranker


class RecoClient(DefaultClient):
    EMBEDDING_MODEL_TYPE = EmbeddingModelTypes.RECOMMENDATION

    def __init__(
        self,
        default_token: str,
        base_columns: List[str] = DEFAULT_COLUMNS,
        detail_columns: List[str] = DEFAULT_DETAIL_COLUMNS,
        output_metric_columns: List[str] = OUTPUT_METRIC_COLUMNS,
        item_docs_path: str = DEFAULT_ITEM_DOCS_PATH,
        lance_db_uri: str = DEFAULT_LANCE_DB_URI,
        user_docs_path: str = DEFAULT_USER_DOCS_PATH,
        re_rank_weight: float = 0.5,
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
            item_docs_path=item_docs_path,
            lance_db_uri=lance_db_uri,
        )
        self.user_docs_path = user_docs_path
        self.default_token = default_token
        self.re_rank_weight = re_rank_weight
        self.re_ranker = None
        self.user_docs = None

    def load(self) -> None:
        super().load()
        self.re_ranker = UserReranker(
            weight=self.re_rank_weight, user_docs=self.user_docs
        )

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

    def postprocess(
        self,
        ranked_items: List[Dict],
        user_id: Optional[str],
        input_item_ids: List[str],
    ) -> List[Dict]:
        return self._add_item_dot_similarities(ranked_items, user_id, input_item_ids)
