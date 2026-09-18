import asyncio

import pandas as pd
from lancedb import connect_async

from constants import (
    DETAIL_COLUMNS,
    N_PROBES,
    NUM_RESULTS,
    REFINE_FACTOR,
    SEMANTIC_RETRIEVAL_UPPER_BOUND,
)

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
        def predicate(k, v):
            # A missing value must be matched with `IS NULL`, not `= 'None'`:
            # the latter compares against the literal string "None" and never
            # matches a real SQL NULL, silently dropping every null-valued row.
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return f"({k} IS NULL)"
            if isinstance(v, bool):
                return f"({k} = {str(v).lower()})"
            if isinstance(v, int):
                return f"({k} = {v})"
            escaped = str(v).replace("'", "''")
            return f"({k} = '{escaped}')"

        return " AND ".join(predicate(k, v) for k, v in filters.items())

    async def search(
        self,
        vector,
        filters: dict,
        n=NUM_RESULTS,
    ) -> pd.DataFrame:
        query = (
            self.table.query()
            .where(self.build_filter(filters))
            .nearest_to(vector)
            .distance_type("cosine")
            .nprobes(N_PROBES)
            .refine_factor(REFINE_FACTOR)
            .select(columns=DETAIL_COLUMNS + DEFAULTS)
            .distance_range(upper_bound=SEMANTIC_RETRIEVAL_UPPER_BOUND)
            .limit(n)
        )
        results = await query.to_pandas(flatten=True)
        results = results.rename(columns={"item_id": "item_id_synchro"})
        return results
