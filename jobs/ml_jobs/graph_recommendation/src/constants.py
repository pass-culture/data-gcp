import os
from collections.abc import Sequence
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = (PROJECT_ROOT / "data").as_posix()
RESULTS_DIR = (PROJECT_ROOT / "results").as_posix()
MLFLOW_RUN_ID_FILEPATH = (PROJECT_ROOT / "results" / "latest_run_id.txt").as_posix()


EMBEDDING_COLUMN = "embedding"
ID_COLUMN = "item_id"
GTL_ID_COLUMN = "gtl_id"
ARTIST_ID_COLUMN = "artist_id"
LANCEDB_NODE_ID_COLUMN = "node_ids"

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
ML_BUCKET_TEMP = f"data-bucket-ml-temp-{ENV_SHORT_NAME}"
