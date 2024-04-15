import typing as t
from docarray import DocumentArray, Document
import lancedb
from filter import Filter
import joblib
import numpy as np
from lancedb.rerankers import Reranker
import pyarrow as pa


DETAIL_COLUMNS = [
    "item_id",
    "topic_id",
    "cluster_id",
    "is_geolocated",
    "booking_number",
    "booking_number_last_7_days",
    "booking_number_last_14_days",
    "booking_number_last_28_days",
    "semantic_emb_mean",
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

DEFAULTS = ["_distance", "_relevance_score", "_user_distance"]


class UserReranker(Reranker):
    def __init__(self, weight, user_docs, return_score="all"):
        super().__init__(return_score)
        self.weight = weight
        self.user_docs = user_docs

    def user_vector(self, var: str) -> Document:
        try:
            return self.user_docs[var]
        except:
            return None

    def user_recommendation(self, vector_results: pa.Table, user_id: str):
        _df = vector_results.to_pandas()
        user_doc = self.user_vector(user_id)
        if user_doc is not None:
            X = vector_results["raw_embeddings"].to_pylist()

            scores = -np.dot(X, user_doc.embedding)
            _df["_user_distance"] = scores

            _df["_relevance_score"] = _df["_distance"] * self.weight + _df[
                "_user_distance"
            ] * (1 - self.weight)
        else:
            _df["_relevance_score"] = _df["_distance"]
        return pa.Table.from_pandas(_df).sort_by("_relevance_score")

    def rerank_hybrid(
        self, query: str, vector_results: pa.Table, fts_results: pa.Table
    ):
        combined_result = self.merge_results(vector_results, fts_results)
        return combined_result

    def rerank_vector(self, query: str, vector_results: pa.Table):
        return self.user_recommendation(vector_results, user_id=query)

    def rerank_fts(self, query: str, fts_results: pa.Table):
        return fts_results


class DefaultClient:
    def load(self) -> None:
        self.item_docs = DocumentArray.load("./metadata/item.docs")
        self.re_ranker = None
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
        user_id: str = None,
        prefilter: bool = True,
        vector_column_name: str = "vector",
        re_rank: bool = False,
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
        )
        if re_rank:
            results = results.rerank(self.re_ranker, query_string=user_id)
        return self.out(results.to_list(), details, item_id=item_id)

    def filter(
        self,
        query_filter: t.Dict = None,
        n=50,
        details: bool = False,
        prefilter: bool = True,
        vector_column_name: str = "booking_number_desc",
        re_rank: bool = False,
        user_id: str = None,
    ) -> t.List[t.Dict]:

        results = (
            self.table.search(
                [0], vector_column_name=vector_column_name, query_type="vector"
            )
            .where(self.build_query(query_filter), prefilter=prefilter)
            .select(columns=self.columns(details))
            .limit(n)
        )
        if re_rank:
            results = results.rerank(self.re_ranker, query_string=user_id)
        return self.out(results.to_list(), details)

    def columns(self) -> t.Optional[t.List[str]]:
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
        self.re_ranker = UserReranker(weight=0.5, user_docs=self.user_docs)

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
