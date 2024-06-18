import typing as t
from docarray import DocumentArray, Document
import lancedb
from filter import Filter
import numpy as np

DETAIL_COLUMNS = [
    "item_id",
    "performer",
]
DEFAULTS = ["_distance"]

<<<<<<< HEAD
=======

>>>>>>> d178ea4b4a0ac2da5f6ab9ee0b3e8024054bef04
class DefaultClient:
    def load(self) -> None:
        self.item_docs = DocumentArray.load("./retrieval_vector/metadata/item.docs")
        uri = "./retrieval_vector/metadata/vector"
        db = lancedb.connect(uri)
        self.table = db.open_table("items")
<<<<<<< HEAD
    
=======

>>>>>>> d178ea4b4a0ac2da5f6ab9ee0b3e8024054bef04
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
<<<<<<< HEAD
        
=======

>>>>>>> d178ea4b4a0ac2da5f6ab9ee0b3e8024054bef04
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
<<<<<<< HEAD
    
=======

>>>>>>> d178ea4b4a0ac2da5f6ab9ee0b3e8024054bef04
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

<<<<<<< HEAD
    
=======

>>>>>>> d178ea4b4a0ac2da5f6ab9ee0b3e8024054bef04
class TextClient(DefaultClient):
    def __init__(self, transformer: str) -> None:
        from sentence_transformers import SentenceTransformer

        self.encoder = SentenceTransformer(transformer)
        # self.reducer = joblib.load(reducer_path)

    def text_vector(self, var: str):
        encode = self.encoder.encode(var)
        encode_array = np.array(encode).flatten()
        # encode_array_normalize=(encode_array/np.linalg.norm(encode_array)).flatten()
        # reduce = np.array(self.reducer.transform([encode])).flatten()
<<<<<<< HEAD
        return Document(embedding=encode_array)
=======
        return Document(embedding=encode_array)
>>>>>>> d178ea4b4a0ac2da5f6ab9ee0b3e8024054bef04
