import typing as t
from docarray import DocumentArray, Document
import lancedb
from linkage_vector.filter import Filter
import numpy as np
import joblib

DETAIL_COLUMNS = [
    "item_id",
    "performer",
]
DEFAULTS = ["_distance"]


class DefaultClient:
    def load(self) -> None:
        self.item_docs = DocumentArray.load("./retrieval_vector/metadata/item.docs")
        uri = "./retrieval_vector/metadata/vector"
        db = lancedb.connect(uri)
        self.table = db.open_table("items")

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
            .where(self.build_query(query_filter), prefilter=prefilter)
            .nprobes(20)
            .refine_factor(10)
            .select(columns=self.columns(details))
            .metric(similarity_metric)
            .limit(n)
            .to_pandas()
        )
        return results

    def build_query(self, params):
        sql = Filter(params).parse_where_clause()
        if len(sql) == 0:
            return None
        return sql

    def columns(self, details: bool) -> t.Optional[t.List[str]]:
        if details:
            return None
        else:
            return DETAIL_COLUMNS


class TextClient(DefaultClient):
    def __init__(self, transformer: str, reducer_path: str) -> None:
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer(transformer)
        self.reducer = joblib.load(reducer_path)

    def text_vector(self, var: str, reduce: bool = True) -> Document:
        encode = self.encoder.encode(var)
        if reduce:
            reduce = np.array(self.reducer.transform([encode])).flatten()
        else:
            reduce = encode.flatten()
        return Document(embedding=reduce)
