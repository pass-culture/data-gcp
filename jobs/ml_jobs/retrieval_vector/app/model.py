import typing as t
from docarray import DocumentArray, Document
import lancedb
from app.filter import Filter


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
        n=50,
        query_filter: t.Dict = None,
        details: bool = False,
        item_id: str = None,
        prefilter: bool = True,
    ) -> t.List[t.Dict]:
        if prefilter:
            vector_column_name = "raw_embeddings"
        else:
            vector_column_name = "vector"

        results = (
            self.table.search(
                vector.embedding,
                vector_column_name=vector_column_name,
                query_type="vector",
            )
            .where(self.build_query(query_filter), prefilter=prefilter)
            .select(columns=self.columns(details))
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

    def columns(self, details):
        if details:
            return None
        else:
            return [
                "item_id",
                "booking_number",
                "example_offer_name",
                "search_group_name",
                "example_offer_id",
            ]

    def out(self, results, details, item_id=None):
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
                row.pop("vector", None)
                predictions.append(
                    dict(
                        {
                            "idx": idx,
                        },
                        **row,
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


class TextClient(DefaultClient):
    def __init__(self, transformer: str) -> None:
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer(transformer)

    def text_vector(self, var: str):
        try:
            return Document(embedding=list(self.encoder.encode(var)))
        except:
            return None
