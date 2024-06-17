import typing as t
from docarray import DocumentArray, Document
from lancedb import connect
import numpy as np
import joblib
import json
from loguru import logger

DETAIL_COLUMNS = [
    "item_id",
    "performer",
]
DEFAULTS = ["_distance"]

# Only keep Text client


class SemanticSpace:
    def __init__(self, bucket_gcs_path) -> None:
        from sentence_transformers import SentenceTransformer

        self.uri = "metadata/vector"
        with open("metadata/model_type.json", "r") as file:
            config = json.load(file)

        self.encoder = SentenceTransformer(config["transformer"])
        self.reducer = joblib.load(config["reducer"])

    def load(self) -> None:
        db = connect(self.uri)
        self.table = db.open_table("items")

    def text_vector(self, var: str, reduce: bool = True) -> Document:
        encode = self.encoder.encode(var)
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
        query_filter: t.Dict = None,
        details: bool = False,
        prefilter: bool = True,
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
        )
        return results
