from typing import List

from docarray import Document, DocumentArray

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
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
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
        )
        self.default_token = default_token
        self.re_ranker = None
        self.user_docs = None

    def load(self) -> None:
        self.item_docs = self.load_item_document()
        self.user_docs = self.load_user_document()
        self.table = self.connect_db()
        self.re_ranker = UserReranker(weight=0.5, user_docs=self.user_docs)

    def load_user_document(self) -> DocumentArray:
        return load_documents("./metadata/user.docs")

    def user_vector(self, var: str) -> Document:
        try:
            return self.user_docs[var]
        except Exception:
            return None
