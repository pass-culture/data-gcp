import time
from typing import Dict, List, Optional

import pandas as pd
from loguru import logger

from app.retrieval.client import DefaultClient
from app.retrieval.constants import (
    DEFAULT_COLUMNS,
    DEFAULT_DETAIL_COLUMNS,
    DEFAULT_ITEM_DOCS_PATH,
    DEFAULT_LANCE_DB_URI,
    OUTPUT_METRIC_COLUMNS,
    EmbeddingModelTypes,
)


class MetadataGraphClient(DefaultClient):
    EMBEDDING_MODEL_TYPE = EmbeddingModelTypes.METADATA_GRAPH

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
        self.item_docs = self.load_item_document()
        logger.info(
            f"Item documents loaded for Metadata Graph Retrieval in {time.time() - start_time:.2f} seconds."
        )

        # Connect to the database
        start_time = time.time()
        self.table = self.connect_db()
        logger.info(f"Connected to database in {time.time() - start_time:.2f} seconds.")

    def postprocess(
        self,
        ranked_items: List[Dict],
        n: int,
        excluded_items: Optional[List[str]] = None,
        **kwargs,
    ):
        postprocessed_items = super().postprocess(
            ranked_items, n, excluded_items, **kwargs
        )
        target = (
            pd.DataFrame(ranked_items)
            .loc[lambda df: df.item_id.isin(excluded_items)]
            .iloc[0]
        )
        target.to_csv("metadata_graph_target.csv")
        post_pro_df = pd.DataFrame(postprocessed_items).assign(
            has_same_artist=lambda df: (
                df.artist_id_list == target.artist_id_list
            ).astype(bool),
            has_same_gtl_l3=lambda df: (df.gtl_l3 == target.gtl_l3).astype(bool),
            has_same_gtl_l4=lambda df: (df.gtl_l4 == target.gtl_l4).astype(bool),
            has_same_series_id=lambda df: (df.series_id == target.series_id).astype(
                bool
            ),
            feature_score=lambda df: df.loc[
                :,
                [
                    "has_same_artist",
                    "has_same_gtl_l3",
                    "has_same_gtl_l4",
                    "has_same_series_id",
                ],
            ].sum(axis=1)
            / target[
                [
                    "artist_id_list",
                    "gtl_l3",
                    "gtl_l4",
                    "series_id",
                ]
            ]
            .notna()
            .sum(),
            # .where(target.gtl_l3.notna(), None),
            # has_same_gtl_l4=lambda df: (df.gtl_l4 == target.gtl_l4).astype(bool)
            # * target.gtl_l4.notna(),
            # has_same_series_id=lambda df: (df.series_id == target.series_id).astype(
            #     bool
            # )
            # * target.series_id.notna(),
            # feature_score=lambda df: (df["has_same_artist", "has_same_gtl_l3"].mean()),
        )

        post_pro_df.to_csv("metadata_graph_retrieval_results.csv", index=False)

        return postprocessed_items
