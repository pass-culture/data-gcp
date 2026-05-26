import os
from collections.abc import Sequence
from pathlib import Path

# GCP project and Environment
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "stg")


# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = (PROJECT_ROOT / "data").as_posix()
RESULTS_DIR = (PROJECT_ROOT / "results").as_posix()
MLFLOW_RUN_ID_FILEPATH = (PROJECT_ROOT / "results" / "latest_run_id.txt").as_posix()

# Column names
EMBEDDING_COLUMN = "embedding"
ID_COLUMN = "item_id"
ITEM_TYPE_COLUMN = "item_type"
GTL_ID_COLUMN = "gtl_id"
ARTIST_ID_COLUMN = "artist_id"
LANCEDB_NODE_ID_COLUMN = "node_ids"
SERIES_ID_COLUMN = "series_id"
MUSIC_LABEL_COLUMN = "music_label"
DISTRIBUTOR_COLUMN = "distributor"
FULL_SCORE_COLUMN = "full_score"
GTL_LABEL_LEVEL_1_COLUMN = "gtl_label_level_1"
GTL_LABEL_LEVEL_2_COLUMN = "gtl_label_level_2"
GTL_LABEL_LEVEL_3_COLUMN = "gtl_label_level_3"
GTL_LABEL_LEVEL_4_COLUMN = "gtl_label_level_4"

# Item types
ITEM_TYPE_BOOK = "book"
ITEM_TYPE_MUSIC = "music"
KNOWN_ITEM_TYPES: Sequence[str] = (ITEM_TYPE_BOOK, ITEM_TYPE_MUSIC)

# Metadata columns that are specific to each item type (GTL labels are namespaced
# per item type to avoid spurious cross-type similarity via identical GTL codes).
GTL_METADATA_COLUMNS: Sequence[str] = (
    GTL_LABEL_LEVEL_1_COLUMN,
    GTL_LABEL_LEVEL_2_COLUMN,
    GTL_LABEL_LEVEL_3_COLUMN,
    GTL_LABEL_LEVEL_4_COLUMN,
)

# GTL columns available per item type.
# Music products only have levels 1 and 2; levels 3 and 4 are always null.
GTL_METADATA_COLUMNS_BY_ITEM_TYPE: dict[str, Sequence[str]] = {
    ITEM_TYPE_BOOK: (
        GTL_LABEL_LEVEL_1_COLUMN,
        GTL_LABEL_LEVEL_2_COLUMN,
        GTL_LABEL_LEVEL_3_COLUMN,
        GTL_LABEL_LEVEL_4_COLUMN,
    ),
    ITEM_TYPE_MUSIC: (
        GTL_LABEL_LEVEL_1_COLUMN,
        GTL_LABEL_LEVEL_2_COLUMN,
    ),
}

# Metadata columns that are shared across all item types (e.g. the same artist_id
# bridges a book and a CD from the same artist).
# Columns that are null for a given item type simply produce no edges for that
# type — no special handling needed.
SHARED_METADATA_COLUMNS: Sequence[str] = (
    ARTIST_ID_COLUMN,
    SERIES_ID_COLUMN,
    MUSIC_LABEL_COLUMN,
)

# Default combined metadata columns (kept for backward-compat with book-only pipelines)
DEFAULT_METADATA_COLUMNS: Sequence[str] = (
    GTL_LABEL_LEVEL_1_COLUMN,
    GTL_LABEL_LEVEL_2_COLUMN,
    GTL_LABEL_LEVEL_3_COLUMN,
    GTL_LABEL_LEVEL_4_COLUMN,
    ARTIST_ID_COLUMN,
    SERIES_ID_COLUMN,
)

# Types
MetadataKey = tuple[str, str]
