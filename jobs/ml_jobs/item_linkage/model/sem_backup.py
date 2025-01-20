import asyncio
import typing as t

from lancedb import connect_async

from constants import DETAIL_COLUMNS, N_PROBES, NUM_RESULTS, REFINE_FACTOR

DEFAULTS = ["_distance"]


class SemanticSpace:
    def __init__(self, model_path: str, linkage_type: str) -> None:
        self.uri = model_path
        self.db = asyncio.run(self.connect_db())
        self.table = asyncio.run(self.open_table(linkage_type))

    async def connect_db(self):
        return await connect_async(self.uri)

    async def open_table(self, linkage_type: str):
        return await self.db.open_table(linkage_type)

    def build_filter(self, filters: dict) -> str:
        return " AND ".join(
            [
                f"{k} = {v}" if isinstance(v, int) else f"{k} = '{v}'"
                for k, v in filters.items()
            ]
        )

    async def search(
        self,
        vector,
        filters: dict,
        similarity_metric="dot",
        n=NUM_RESULTS,
        vector_column_name: str = "vector",
    ) -> t.List[t.Dict]:
        query = (
            self.table.query(
                # vector_column_name=vector_column_name,
            )
            .where(self.build_filter(filters))
            # .where(self.build_filter(filters), prefilter=True)
            .nearest_to(vector)
            # .nearest_to(vector, metric=similarity_metric)
            .nprobes(N_PROBES)
            .refine_factor(REFINE_FACTOR)
            .select(columns=DETAIL_COLUMNS)
            # .metric(similarity_metric)
            .limit(n)
            # .rename(columns={"item_id": "item_id_synchro"})
        )
        results = await query.to_pandas(flatten=True)
        results = results.rename(columns={"item_id": "item_id_synchro"})
        return results
