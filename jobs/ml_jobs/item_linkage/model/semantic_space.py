import json
import typing as t

import joblib
import numpy as np
from docarray import Document
from lancedb import connect
from sentence_transformers import SentenceTransformer
from constants import N_PROBES, REFINE_FACTOR, NUM_RESULTS, MODEL_TYPE as config

DETAIL_COLUMNS = [
    "item_id",
    "performer",
]
DEFAULTS = ["_distance"]


class SemanticSpace:
    def __init__(self, model_path: str) -> None:
        self.uri = model_path
        self._encoder = SentenceTransformer(config["transformer"])
        self.hnne_reducer = joblib.load(config["reducer_pickle_path"])

        db = connect(self.uri)
        self.table = db.open_table("items")

    def search(
        self,
        vector: Document,
        similarity_metric="dot",
        n=NUM_RESULTS,
        vector_column_name: str = "vector",
    ) -> t.List[t.Dict]:
        results = (
            self.table.search(
                vector.embedding,
                vector_column_name=vector_column_name,
                query_type="vector",
            )
            .nprobes(N_PROBES)
            .refine_factor(REFINE_FACTOR)
            .select(columns=DETAIL_COLUMNS)
            .metric(similarity_metric)
            .limit(n)
            .to_pandas(flatten=True)
            .rename(columns={"item_id": "item_id_candidate"})
        )
        return results
