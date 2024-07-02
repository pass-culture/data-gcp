import json
import typing as t

import joblib
import numpy as np
from docarray import Document
from lancedb import connect
from sentence_transformers import SentenceTransformer

DETAIL_COLUMNS = [
    "item_id",
    "performer",
]
DEFAULTS = ["_distance"]

# Only keep Text client


class SemanticSpace:
    def __init__(self, model_path: str) -> None:
        self.uri = model_path
        with open("metadata/model_type.json", "r") as file:
            config = json.load(file)

        self._encoder = SentenceTransformer(config["transformer"])
        self.hnne_reducer = joblib.load(config["reducer_pickle_path"])

    def load(self) -> None:
        db = connect(self.uri)
        self.table = db.open_table("items")

    def text_vector(self, var: str, reduce: bool = True) -> Document:
        encode = self._encoder.encode(var)
        if reduce:
            reduce = np.array(self.reducer.transform([encode])).flatten()
        else:
            reduce = encode.flatten()
        return Document(embedding=reduce)

    def search(
        self,
        vector: Document,
        similarity_metric="dot",
        n=50,
        vector_column_name: str = "vector",
    ) -> t.List[t.Dict]:
        results = (
            self.table.search(
                vector.embedding,
                vector_column_name=vector_column_name,
                query_type="vector",
            )
            .nprobes(20)
            .refine_factor(10)
            .select(columns=DETAIL_COLUMNS)
            .metric(similarity_metric)
            .limit(n)
            .to_pandas(flatten=True)
            .rename(columns={"item_id": "linked_item_id"})
        )
        return results
