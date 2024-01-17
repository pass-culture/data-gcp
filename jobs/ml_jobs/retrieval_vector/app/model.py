import typing as t
from docarray import DocumentArray, Document
import lancedb
from filter import Filter
import joblib
import numpy as np


DETAIL_COLUMNS = [
    "item_id",
    "topic_id",
    "cluster_id",
    "is_geolocated",
    "booking_number",
    "stock_price",
    "offer_creation_date",
    "stock_beginning_date",
    "category",
    "subcategory_id",
    "search_group_name",
    "gtl_id",
    "gtl_l3",
    "gtl_l4",
    "total_offers",
    "example_offer_id",
    "example_venue_id",
    "example_offer_name",
    "example_venue_latitude",
    "example_venue_longitude",
]

DEFAULTS = ["_distance"]


class DefaultClient:
    def load(self) -> None:
        self.item_docs = DocumentArray.load("./metadata/item.docs")
        uri = "./metadata/vector"
        db = lancedb.connect(uri)
        self.table = db.open_table("items")

    def offer_vector(self, var: str) -> Document:
        # not default case
        try:
            return self.item_docs[var]
        except:
            return None

    def build_query(self, params):
        sql = Filter(params).parse_where_clause()
        if len(sql) == 0:
            return None
        return sql

    def search(
        self,
        vector: Document,
        similarity_metric="dot",
        n=50,
        query_filter: t.Dict = None,
        details: bool = False,
        item_id: str = None,
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
            .to_list()
        )
        return self.out(results, details, item_id=item_id)

    def filter(
        self,
        query_filter: t.Dict = None,
        n=50,
        details: bool = False,
        prefilter: bool = True,
    ) -> t.List[t.Dict]:
        results = (
            self.table.search(
                [0], vector_column_name="booking_number_desc", query_type="vector"
            )
            .where(self.build_query(query_filter), prefilter=prefilter)
            .select(columns=self.columns(details))
            .limit(n)
            .to_list()
        )
        return self.out(results, details)

    def columns(self, details: bool) -> t.Optional[t.List[str]]:
        if details:
            return None
        else:
            return DETAIL_COLUMNS

    def out(self, results, details: bool, item_id: str = None):
        predictions = []
        for idx, row in enumerate(results):
            if item_id is not None and str(row["item_id"]) == item_id:
                continue

            if not details:
                predictions.append(
                    {
                        "idx": idx,
                        "item_id": row["item_id"],
                    }
                )
            else:
                # drop embs to reduce latency
                row.pop("vector", None)
                row.pop("raw_embeddings", None)
                predictions.append(
                    dict(
                        {
                            "idx": idx,
                        },
                        **{k: row[k] for k in row if k in DETAIL_COLUMNS + DEFAULTS}
                    )
                )
        return predictions


class RecoClient(DefaultClient):
    def __init__(self, default_token: str) -> None:
        self.default_token = default_token

    def user_vector(self, var: str) -> Document:
        default_user_embbeding = self.user_docs[self.default_token]
        try:
            return self.user_docs[var]
        except:
            return default_user_embbeding

    def load(self) -> None:
        self.item_docs = DocumentArray.load("./metadata/item.docs")
        self.user_docs = DocumentArray.load("./metadata/user.docs")
        uri = "./metadata/vector"
        db = lancedb.connect(uri)
        self.table = db.open_table("items")


class TextClient(DefaultClient):
    def __init__(self, transformer: str, reducer_path: str) -> None:
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer(transformer)
        self.reducer = joblib.load(reducer_path)

    def text_vector(self, var: str):
        encode = self.encoder.encode(var)
        reduce = np.array(self.reducer.transform([encode])).flatten()
        return Document(embedding=reduce)
