import time
from typing import List

from loguru import logger

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    DEFAULT_ITEM_DOCS_PATH,
    DEFAULT_LANCE_DB_URI,
    OUTPUT_METRIC_COLUMNS,
)


class MetadataGraphClient(DefaultClient):
    def __init__(
        self,
        default_token: str,
        base_columns: List[str] = DEFAULT_COLUMNS,
        detail_columns: List[str] = DEFAULT_DETAIL_COLUMNS,
        output_metric_columns: List[str] = OUTPUT_METRIC_COLUMNS,
        item_docs_path: str = DEFAULT_ITEM_DOCS_PATH,
        lance_db_uri: str = DEFAULT_LANCE_DB_URI,
    ) -> None:
        super().__init__(
            base_columns=base_columns,
            detail_columns=detail_columns,
            output_metric_columns=output_metric_columns,
            item_docs_path=item_docs_path,
            lance_db_uri=lance_db_uri,
        )
        self.default_token = default_token

    def load(self) -> None:
        """
        Load only item documents and connect to the database.
        Overrides the load method in the DefaultClient class, not to load the user documents.
        """

        # Load only item documents
        start_time = time.time()
        self.load_item_document()
        logger.info(f"Item documents loaded in {time.time() - start_time:.2f} seconds.")

        # Connect to the database
        start_time = time.time()
        self.table = self.connect_db()
        logger.info(f"Connected to database in {time.time() - start_time:.2f} seconds.")
