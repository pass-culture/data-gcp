import typing as t
from docarray import DocumentArray, Document
from annlite import AnnLite
import uuid

dtypes = {
    "category": "str",
    "subcategory_id": "str",
    "search_group_name": "str",
    "is_numerical": "float",
    "is_national": "float",
    "is_geolocated": "float",
    "offer_is_duo": "float",
    "booking_number": "float",
    "stock_price": "float",
    "offer_creation_date": "float",
    "stock_beginning_date": "float",
    "is_underage_recommendable": "float",
}

filter_dtypes = {
    "$eq": "float",
    "$lte": "float",
    "$gte": "float",
}
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
        
    def parse_params(self, params):
        result = {}
        for key, value in params.items():
            if isinstance(value, dict):
                result[key] = self.parse_params(value, filter_dtypes)
            else:
                data_type = filter_dtypes.get(key)
                if data_type == "str":
                    result[key] = str(value)
                elif data_type == "float":
                    result[key] = float(value)
                else:
                    result[key] = value
        return result

    def index(self):
        self.ann = AnnLite(
            self.n_dim,
            metric=self.metric,
            data_path=f"./metadata/annlite_{str(uuid.uuid4())}",
            ef_construction=200,  # default
            ef_search=500,  # limit size
            max_connection=48,  # higher better
            columns=dtypes,
        )
        self.ann.index(self.item_docs)

    def search(
        self,
        vector: Document,
        n=50,
        query_filter: t.Dict = None,
        details: bool = False,
        item_id: str = None,
    ) -> t.List[t.Dict]:
        docs = DocumentArray(vector)
        self.ann.search(docs, filter=query_filter, limit=n)
        results = docs[0].matches
        predictions = []
        for idx, row in enumerate(results):
            # don't retrieve same object.
            if item_id is not None and str(row.tags["item_id"]) == item_id:
                continue

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
            return Document(embedding=list(self.encoder.encode(var)))
        except:
            return None
