import typing as t

from lancedb import connect

from constants import DETAIL_COLUMNS, N_PROBES, NUM_RESULTS, REFINE_FACTOR

DEFAULTS = ["_distance"]


class SemanticSpace:
    def __init__(self, model_path: str, linkage_type: str) -> None:
        self.uri = model_path
        db = connect(self.uri)
        self.table = db.open_table(linkage_type)

    def build_filter(self, filters: list) -> str:
        return " AND ".join([f"{feature} = '{feature}'" for feature in filters])

    def search(
        self,
        vector,
        filters: list,
        similarity_metric="dot",
        n=NUM_RESULTS,
        vector_column_name: str = "vector",
    ) -> t.List[t.Dict]:
        results = (
            self.table.search(
                vector,
                # vector_column_name=vector_column_name,
            )
            .where(self.build_filter(filters), prefilter=True)
            .nprobes(N_PROBES)
            .refine_factor(REFINE_FACTOR)
            .select(columns=DETAIL_COLUMNS)
            .metric(similarity_metric)
            .limit(n)
            .to_pandas(flatten=True)
            .rename(columns={"item_id": "item_id_synchro"})
        )
        return results
