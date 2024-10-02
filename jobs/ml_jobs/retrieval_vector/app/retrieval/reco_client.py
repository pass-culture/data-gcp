from typing import List, Optional

from docarray import Document, DocumentArray

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    DEFAULT_ITEM_DOCS_PATH,
    DEFAULT_LANCE_DB_URI,
    DEFAULT_USER_DOCS_PATH,
    OUTPUT_METRIC_COLUMNS,
)
from app.retrieval.core.reranker import UserReranker
from app.retrieval.utils import load_documents


class RecoClient(DefaultClient):
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
        self.item_docs = self.load_item_document()
        self.user_docs = self.load_user_document()
        self.table = self.connect_db()
        self.re_ranker = UserReranker(
            weight=self.re_rank_weight, user_docs=self.user_docs
        )

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
