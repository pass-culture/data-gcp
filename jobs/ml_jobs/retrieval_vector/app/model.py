import typing as t
from docarray import DocumentArray, Document
from annlite import AnnLite
import uuid


class DefaultClient:
    def __init__(self, metric: str, n_dim: int) -> None:
        self.metric = metric
        self.n_dim = n_dim

    def load(self) -> None:
        self.item_docs = DocumentArray.load("./metadata/item.docs")

    def offer_vector(self, var: str) -> Document:
        # not default case
        try:
            return self.item_docs[var]
        except:
            return None

    def index(self):
        self.ann = AnnLite(
            self.n_dim,
            metric=self.metric,
            data_path=f"./metadata/annlite_{str(uuid.uuid4())}",
            ef_construction=200,  # default
            ef_search=500,  # limit size
            max_connection=48,  # higher better
            columns={
                "category": "str",
                "subcategory_id": "str",
                "search_group_name": "str",
                "is_numerical": "int",
                "is_national": "int",
                "is_geolocated": "int",
                "offer_is_duo": "int",
                "booking_number": "float",
                "stock_price": "float",
                "offer_creation_date": "int",
                "stock_beginning_date": "int",
            },
        )
        self.ann.index(self.item_docs)

    def search(
        self, vector: Document, n=50, query_filter: t.Dict = None, details: bool = False
    ) -> t.List[t.Dict]:
        docs = DocumentArray(vector)
        self.ann.search(docs, filter=query_filter, limit=n)
        results = docs[0].matches
        predictions = []
        for idx, row in enumerate(results):
            if not details:
                predictions.append(
                    {
                        "idx": idx,
                        "score": float(row.scores[self.metric].value),
                        "item_id": row.tags["item_id"],
                    }
                )
            else:
                predictions.append(
                    dict(
                        {
                            "idx": idx,
                            "score": float(row.scores[self.metric].value),
                        },
                        **row.tags,
                    )
                )
        return predictions

    def filter(
        self,
        query_filter: t.Dict,
        n=50,
        details: bool = False,
        order_by: str = "booking_number",
        ascending: bool = False,
    ) -> t.List[t.Dict]:

        results = self.ann.filter(
            filter=query_filter, limit=n, order_by=order_by, ascending=ascending
        )
        predictions = []
        for idx, row in enumerate(results):
            if not details:
                predictions.append(
                    {
                        "idx": idx,
                        "item_id": row.tags["item_id"],
                    }
                )
            else:
                predictions.append(
                    dict(
                        {
                            "idx": idx,
                        },
                        **row.tags,
                    )
                )
        return predictions


class RecoClient(DefaultClient):
    def __init__(self, metric: str, n_dim: int, default_token: str) -> None:
        super().__init__(metric, n_dim)
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
    def __init__(self, metric: str, n_dim: int, transformer: str) -> None:
        super().__init__(metric, n_dim)
        # import only for custom model
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer(transformer)

    def text_vector(self, var: str):
        try:
            return Document(
                embedding=list(self.encoder.encode(var))
                + list(self.encoder.encode(var))
            )
        except:
            return None
