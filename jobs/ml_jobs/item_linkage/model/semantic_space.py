import re
import asyncio
import typing as t
from lancedb import connect_async

from constants import DETAIL_COLUMNS, N_PROBES, NUM_RESULTS, REFINE_FACTOR


class SemanticSpace:
    def __init__(self, db, table, max_concurrency: int = 10):
        """
        Semantic space wrapper around LanceDB table.

        Args:
            db: LanceDB async connection
            table: LanceDB table
            max_concurrency: Maximum concurrent search queries
        """
        self.db = db
        self.table = table

        # Concurrency control to avoid:
        # - Too many open files
        # - Excessive parquet fragment scanning
        # - File descriptor exhaustion
        self.semaphore = asyncio.Semaphore(max_concurrency)

    @classmethod
    async def create(
        cls,
        model_path: str,
        linkage_type: str,
        max_concurrency: int = 10,
    ):
        """
        Async factory to properly initialize DB and table
        inside the correct event loop.
        """
        db = await connect_async(model_path)
        table = await db.open_table(linkage_type)
        return cls(db, table, max_concurrency)

    def build_filter(self, filters: dict) -> str:
        """
        Build SQL-style filter string from dictionary.
        """
        return " AND ".join(
            [
                f"({k} = {v})" if isinstance(v, int)
                else f"({k} = '{v}')"
                for k, v in filters.items()
            ]
        )

    async def search(
        self,
        vector,
        filters: dict,
        n = NUM_RESULTS,
    ) -> t.List[dict]:
        """
        Perform nearest neighbor search with controlled concurrency.
        """

        async with self.semaphore:
            query = (
                self.table.query()
                .where(self.build_filter(filters))
                .nearest_to(vector)
                .distance_type("cosine")
                .nprobes(N_PROBES)
                .refine_factor(REFINE_FACTOR)
                .select(columns=DETAIL_COLUMNS)
                .limit(n)
            )

            results = await query.to_pandas(flatten=True)
            return results.rename(columns={"item_id": "item_id_synchro"})
