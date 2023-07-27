import typing as t
from docarray import DocumentArray, Document


class RecoClient:
    def __init__(
        self, type: str, metric: str, default_token: str, n_dim: int, ascending: bool
    ) -> None:
        self.type = type
        self.metric = metric
        self.default_token = default_token
        self.n_dim = n_dim
        self.ascending = ascending
        self.load()

    def vector(self, input: str):
        default_user_embbeding = self.user_docs[self.default_token]
        try:
            return self.user_docs[input]
        except:
            return default_user_embbeding

    def load(self) -> None:
        self.item_docs = DocumentArray.load("./metadata/item.docs")
        self.user_docs = DocumentArray.load("./metadata/user.docs")

    def search(
        self, vector: Document, n=50, query_filter: t.Dict = None, details: bool = False
    ) -> t.List[t.Dict]:

        results = self.item_docs.find(
            vector, filter=query_filter, limit=n, ascending=self.ascending
        )[0]
        predictions = []
        for idx, row in enumerate(results):
            if not details:
                predictions.append(
                    {
                        "idx": idx,
                        "score": row.scores[self.metric].value,
                        "item_id": row.tags["item_id"],
                    }
                )
            else:
                predictions.append(
                    dict(
                        {
                            "idx": idx,
                            "score": row.scores[self.metric].value,
                        },
                        **row.tags
                    )
                )
        return predictions


class SemanticClient(RecoClient):
    def __init__(self, type: str, metric: str, default_token: str, n_dim: int) -> None:
        super().__init__(type, metric, default_token, n_dim)
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer("all-MiniLM-L6-v2")

    def vector(self, input: str):
        return self.encoder.encode(input)

    def load(self):
        self.item_docs = DocumentArray.load("./metadata/item.docs")
