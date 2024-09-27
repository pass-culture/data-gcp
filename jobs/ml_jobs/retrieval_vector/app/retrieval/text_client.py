from typing import List, Optional

import joblib
import numpy as np
from docarray import Document
from sentence_transformers import SentenceTransformer

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    OUTPUT_METRIC_COLUMNS,
)


class TextClient(DefaultClient):
    def __init__(
        self,
        transformer: str,
        reducer_path: Optional[str] = None,
        base_columns: List[str] = DEFAULT_COLUMNS,
        detail_columns: List[str] = DEFAULT_DETAIL_COLUMNS,
        output_metric_columns: List[str] = OUTPUT_METRIC_COLUMNS,
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
        )
        self.encoder = SentenceTransformer(transformer)
        self.reducer = joblib.load(reducer_path) if reducer_path else None

    def text_vector(self, text: str) -> Document:
        encoded_vector = self.encoder.encode(text)
        if self.reducer:
            encoded_vector = np.array(
                self.reducer.transform([encoded_vector])
            ).flatten()
        return Document(embedding=encoded_vector)
