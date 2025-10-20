import os
from collections.abc import Sequence

ID_COLUMN = "item_id"
GTL_ID_COLUMN = "gtl_id"
DEFAULT_METADATA_COLUMNS: Sequence[str] = (
    "gtl_label_level_1",
    "gtl_label_level_2",
    "gtl_label_level_3",
    "gtl_label_level_4",
    "artist_id",
)

MetadataKey = tuple[str, str]


GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-prod")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
BIGQUERY_EMBEDDING_DATASET = f"ml_preproc_{ENV_SHORT_NAME}"
BIGQUERY_EMBEDDING_TABLE = "graph_embeddings"
