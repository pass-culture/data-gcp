import lancedb
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from loguru import logger
from pyarrow.fs import GcsFileSystem

from app.constants import PARQUET_FILE, embedding_model
from app.search.filters import apply_filters

PARTITION_COLS = ["offer_subcategory_id"]


class SearchClient:
    def __init__(self, database_uri: str, vector_table: str, scalar_table: str):
        """Connects to LanceDB and opens the specified table."""
        self.embedding_model = embedding_model
        logger.info(f"Connecting to LanceDB at: {database_uri}")
        self.db = lancedb.connect(database_uri)

        logger.info(f"Opening vector table: {vector_table}")
        self.vector_table = self.db.open_table(vector_table)

        # Cache the global dataset once at startup
        logger.info("Loading global dataset for fast partition pruning...")
        logger.info(f"Using parquet file: {PARQUET_FILE}")

        gcs = GcsFileSystem()
        gcs_path = PARQUET_FILE.replace("gs://", "")

        # NOTE: Ensure these types match your actual data!
        hive_partitioning = ds.partitioning(
            flavor="hive", schema=pa.schema([("offer_subcategory_id", pa.string())])
        )

        self.dataset = ds.dataset(
            gcs_path,
            filesystem=gcs,
            format="parquet",
            partitioning=hive_partitioning,
        )
        self.global_lf = pl.scan_pyarrow_dataset(self.dataset)

    # ── Public API ───────────────────────────────────────────────────────

    def table_query(self, k: int = 1000, filters: list[dict] | None = None):
        """
        Scalar search with optional filters. Relies on native Polars partition pruning.
        """
        lf = self.global_lf

        if filters:
            lf = apply_filters(lf, filters)

        return lf.head(k).select(["item_id", "offer_id"]).collect().to_dicts()

    def vector_search(self, query_vector, k: int = 5):
        """Performs a vector similarity search using LanceDB."""
        return (
            self.vector_table.search(query_vector)
            .nprobes(10)
            .select(
                [
                    "id",
                    "offer_name",
                    "offer_description",
                    "offer_subcategory_id",
                ]
            )
            .limit(k)
            .to_list()
        )
