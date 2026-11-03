"""Extraction logic used by `cli/extraction.py::extract`.

Kept out of the CLI entrypoint so this logic can be unit-tested and reused
directly, without going through Typer. The underlying HTTP fetch/retry
machinery against QLever itself lives in src/extraction/qlever.py — this module
is the Wikidata-domain layer on top of it (query rendering, entity ID
normalization).
"""

import pandas as pd
from loguru import logger

from src.extraction.constants import WIKIDATA_ENTITY_PREFIX
from src.extraction.qlever import fetch_wikidata_qlever_csv
from src.extraction.wikidata_config import QUERY_CONFIGS, render_query


def extract_wikidata_id(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        wikidata_id=lambda df: df.wikidata_id.str.replace(
            WIKIDATA_ENTITY_PREFIX, "", regex=True
        ),
    )


def extract_query(query_name: str) -> pd.DataFrame:
    """Fetch one extraction target with a single QLever query."""
    query_string = render_query(query_name)
    logger.debug(f"SPARQL Query: \n{query_string}")
    return fetch_wikidata_qlever_csv(
        query_string, dtype=QUERY_CONFIGS[query_name].id_column_dtypes
    ).pipe(extract_wikidata_id)
