"""Per-domain config used to render the Jinja SPARQL templates in `queries/`.

Each Wikidata extraction query (music, book, movie, gkg...) shares the same overall
shape (entity-type filter, external-ID matching, multi-valued attribute subqueries)
but differs in which Wikidata entity types and external-ID properties it targets.
This module captures those differences so the templates stay DRY.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from jinja2 import Environment, FileSystemLoader

from src.extraction.constants import (
    BOOK_ID_PROPERTIES,
    GKG_ID_PROPERTIES,
    MOVIE_ID_PROPERTIES,
    MUSIC_ENTITY_TYPES,
    MUSIC_ID_PROPERTIES,
    MUSIC_IDS_KEY,
    PERSON_ENTITY_TYPES,
    IdProperty,
)

TEMPLATE_DIR = Path(__file__).resolve().parent / "queries"


@dataclass(frozen=True)
class QueryConfig:
    """Config used to render one query from a template in `queries/`.

    `base_mode` only applies to the "extract_artists.rq.j2" template:
      - "scored": the base subquery only resolves `?matching_score` from
        `id_properties`; single-valued attributes (labels, description, wikipedia...)
        are fetched as separate top-level OPTIONALs.
      - "grouped": single-valued attributes are grouped together with the entity
        filter in one subquery, and no `?matching_score` column is produced (used by
        `music`, whose matching score is computed separately by `music_ids`).

    `optional`, when true, means a genuinely empty/missing result for this target
    is expected, not a failure: `extract` skips saving a raw file instead of
    raising, and `merge` skips it instead of hard-failing the whole merge. Used by
    `music_ids`, whose own query can legitimately return nothing (see
    `merge_data`'s pre-merge step in src/extraction/wikidata_merge.py).
    """

    template: str
    entity_types: list[str]
    id_properties: list[IdProperty] = field(default_factory=list)
    base_mode: Literal["scored", "grouped"] = "scored"
    optional: bool = False

    @property
    def id_column_dtypes(self) -> dict[str, type]:
        """Read external-ID columns as strings, never inferred: Wikidata holds
        malformed values among otherwise numeric IDs (e.g. Deezer IDs entered as
        URLs), and read_csv's per-chunk inference would then mix floats and
        strings in one column — unwritable to parquet, and "123" mangled to 123.0.
        """
        return {id_property.var: str for id_property in self.id_properties}


QUERY_CONFIGS: dict[str, QueryConfig] = {
    "music": QueryConfig(
        template="extract_artists.rq.j2",
        entity_types=MUSIC_ENTITY_TYPES,
        id_properties=MUSIC_ID_PROPERTIES,
        base_mode="grouped",
    ),
    MUSIC_IDS_KEY: QueryConfig(
        template="extract_artist_ids.rq.j2",
        entity_types=MUSIC_ENTITY_TYPES,
        id_properties=MUSIC_ID_PROPERTIES,
        optional=True,
    ),
    "book": QueryConfig(
        template="extract_artists.rq.j2",
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=BOOK_ID_PROPERTIES,
    ),
    "movie": QueryConfig(
        template="extract_artists.rq.j2",
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=MOVIE_ID_PROPERTIES,
    ),
    # IDs only: downstream only uses the `gkg` flag merge_data derives from these
    # wikidata_ids (a notability tie-breaker in src/linkage), not their attributes.
    "gkg": QueryConfig(
        template="extract_artist_ids.rq.j2",
        entity_types=PERSON_ENTITY_TYPES,
        id_properties=GKG_ID_PROPERTIES,
    ),
}

# autoescape is off on purpose: these templates render SPARQL query text (not HTML)
# from the static QUERY_CONFIGS below, not from user input. HTML-escaping would
# corrupt the query syntax (e.g. the `<...>` IRIs and `&` in Wikidata property URIs).
_jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
    autoescape=False,
)


def render_query(query_name: str) -> str:
    config = QUERY_CONFIGS[query_name]
    rendered_template = _jinja_env.get_template(config.template)
    return rendered_template.render(
        entity_types=config.entity_types,
        id_properties=config.id_properties,
        base_mode=config.base_mode,
    )
