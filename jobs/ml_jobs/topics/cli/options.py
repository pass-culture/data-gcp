"""Options shared by several commands, defined once so their defaults and
help texts stay in sync."""

import typer

from src.config import GCP_PROJECT_ID, GTL_LEVELS, ML_INPUT_DATASET
from src.nlp_preprocessing import DEFAULT_ENTITY_TYPES

PROJECT = typer.Option(GCP_PROJECT_ID, help="BigQuery project (env: GCP_PROJECT_ID)")
ML_INPUT = typer.Option(
    ML_INPUT_DATASET,
    help="BigQuery dataset holding item_metadata (env: ENV_SHORT_NAME)",
)
CACHE_DIR = typer.Option(
    "data", help="Directory caching BigQuery tables (items, embeddings), per table"
)

# load-data / run
SAMPLE_SIZE = typer.Option(30_000, min=1, help="Total items to sample")
STRATA_LEVELS = typer.Option(
    3,
    min=1,
    max=len(GTL_LEVELS),
    help="Recursively normalize sampling across this many GTL levels "
    "(1 = level-1 only, 3 = level-1 > level-2 > level-3)",
)
SAMPLE_SEED = typer.Option(42, help="Random seed for the sample draw")
LANG = typer.Option(
    "fr", help="Keep only this detected language before sampling (all = no filter)"
)
REFRESH = typer.Option(
    False, help="Re-query BigQuery even if a cached item table exists"
)
SAMPLE_OUT = typer.Option(
    "data/sample", help="Output path stem: writes <stem>.parquet + <stem>.csv"
)

# preprocess / run
ENRICHED_OUT = typer.Option(
    "data/nlp_enriched", help="Output path stem: writes <stem>.parquet + <stem>.csv"
)
ENTITY_TYPES = typer.Option(
    ",".join(DEFAULT_ENTITY_TYPES),
    help="Comma-separated entity types to keep, labels unified across "
    "languages (see ENTITY_LABEL_ALIASES in src/nlp_preprocessing.py)",
)


def parse_entity_types(value: str) -> list[str]:
    """--entity-types "person, org" -> ["PERSON", "ORG"]."""
    return [t.strip().upper() for t in value.split(",") if t.strip()]
